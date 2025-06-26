package shell

import (
	"errors"
	"fmt"
	"index/suffixarray"
	"io"
	"slices"
	"strconv"
	"strings"
	"unicode"

	"github.com/jpappel/atlas/pkg/query"
	"golang.org/x/term"
)

type Interpreter struct {
	State   State
	Workers uint
	env     map[string]string
	term    *term.Terminal
	tab     struct {
		inTabMode   bool
		completions []string
		pos         int
		index       *suffixarray.Index
	}
}

type ITokType int

const (
	ITOK_INVALID ITokType = iota

	ITOK_VAR_NAME

	// values
	ITOK_VAL_INT
	ITOK_VAL_STR
	ITOK_VAL_TOKENS
	ITOK_VAL_CLAUSE

	// commands
	ITOK_CMD_HELP
	ITOK_CMD_CLEAR
	ITOK_CMD_EXIT
	ITOK_CMD_ENV
	ITOK_CMD_LET
	ITOK_CMD_DEL
	ITOK_CMD_PRINT
	ITOK_CMD_LEN
	ITOK_CMD_SLICE
	ITOK_CMD_REMATCH
	ITOK_CMD_REPATTERN
	ITOK_CMD_OPTIMIZE
	ITOK_CMD_TOKENIZE
	ITOK_CMD_PARSE
	ITOK_CMD_COMPILE
)

type IToken struct {
	Type ITokType
	Text string
}

func NewInterpreter(initialState State, env map[string]string, workers uint) *Interpreter {
	return &Interpreter{
		State:   initialState,
		env:     env,
		Workers: workers,
	}
}

func (inter *Interpreter) Reset() {
	inter.State = make(State)
}

func (inter *Interpreter) Eval(w io.Writer, tokens []IToken) (bool, error) {
	if len(tokens) == 0 {
		return false, nil
	}

	if slices.ContainsFunc(tokens, func(token IToken) bool {
		return token.Type == ITOK_INVALID
	}) {
		b := strings.Builder{}
		b.WriteString("Unexpected token(s) at ")
		for i, t := range tokens {
			if t.Type == ITOK_INVALID {
				b.WriteString(fmt.Sprint(i, ", "))
			}
		}
		return false, errors.New(b.String())
	}

	var variableName string
	var carryValue Value
	var ok bool
out:
	for i := len(tokens) - 1; i >= 0; i-- {
		t := tokens[i]
		switch t.Type {
		case ITOK_CMD_HELP:
			printHelp(w)
			break out
		case ITOK_CMD_EXIT:
			return true, nil
		case ITOK_CMD_CLEAR:
			fmt.Fprint(w, "\033[H\033[J")
			break out
		case ITOK_CMD_ENV:
			if t.Text == "" {
				for k, v := range inter.env {
					fmt.Fprintln(w, k, ":", v)
				}
			} else {
				v, ok := inter.env[t.Text]
				if !ok {
					return false, fmt.Errorf("No env var: %s", t.Text)
				}
				fmt.Fprintln(w, t.Text, ":", v)
			}
			break out
		case ITOK_CMD_LET:
			if variableName != "" {
				inter.State[variableName] = carryValue
				carryValue.Type = VAL_INVALID
			}
			break out
		case ITOK_CMD_DEL:
			if len(tokens) == 1 {
				fmt.Fprintln(w, "Deleting all variables")
				inter.State = make(State)
			} else {
				// HACK: variable name is not evaluated correctly so just look at the next token
				delete(inter.State, tokens[i+1].Text)
			}
			carryValue.Type = VAL_INVALID
			break out
		case ITOK_CMD_PRINT:
			if len(tokens) == 1 {
				fmt.Fprintln(w, "Variables:")
				fmt.Fprintln(w, inter.State)
			} else {
				carryValue, ok = inter.State[tokens[1].Text]
				if !ok {
					return false, fmt.Errorf("No variable %s", tokens[1].Text)
				}
			}
		case ITOK_CMD_REMATCH:
			if carryValue.Type != VAL_STRING {
				return false, fmt.Errorf("Unable to march against argument of type: %s", carryValue.Type)
			}

			body, ok := carryValue.Val.(string)
			if !ok {
				return true, errors.New("Type corruption during rematch, expected string")
			}

			b := strings.Builder{}
			matchGroupNames := query.LexRegex.SubexpNames()
			for _, match := range query.LexRegex.FindAllStringSubmatch(body, -1) {
				for i, part := range match {
					b.WriteString(matchGroupNames[i])
					fmt.Fprintf(&b, "[%d]", len(part))
					b.WriteByte(':')
					b.WriteString(part)
					b.WriteByte('\n')
				}
				b.WriteByte('\n')
			}
			carryValue.Val = b.String()
		case ITOK_CMD_REPATTERN:
			fmt.Fprintln(w, query.LexRegexPattern)
			break out
		case ITOK_CMD_TOKENIZE:
			if carryValue.Type != VAL_STRING {
				return false, fmt.Errorf("Unable to tokenize argument of type: %s", carryValue.Type)
			}

			rawQuery, ok := carryValue.Val.(string)
			if !ok {
				return true, errors.New("Type corruption during tokenize, expected string")
			}
			carryValue.Type = VAL_TOKENS
			carryValue.Val = query.Lex(rawQuery)
		case ITOK_CMD_PARSE:
			if carryValue.Type != VAL_TOKENS {
				return false, fmt.Errorf("Unable to parse argument of type: %s", carryValue.Type)
			}

			queryTokens, ok := carryValue.Val.([]query.Token)
			if !ok {
				return true, errors.New("Type corruption during parse, expected []query.Tokens")
			}

			clause, err := query.Parse(queryTokens)
			if err != nil {
				return false, err
			}
			carryValue.Type = VAL_CLAUSE
			carryValue.Val = clause
		case ITOK_CMD_OPTIMIZE:
			if carryValue.Type != VAL_CLAUSE {
				return false, fmt.Errorf("Unable to optimize argument of type: %s", carryValue)
			}

			clause, ok := carryValue.Val.(*query.Clause)
			if !ok {
				return true, errors.New("Type corruption during optimization, expected *query.Clause")
			}

			o := query.NewOptimizer(clause, inter.Workers)
			switch t.Text {
			case "simplify":
				o.Simplify()
			case "tighten":
				o.Tighten()
			case "flatten":
				o.Flatten()
			case "sort":
				o.SortStatements()
			case "tidy":
				o.Tidy()
			case "contradictions":
				o.Contradictions()
			case "compact":
				o.Compact()
			case "strictEq":
				o.StrictEquality()
			default:
				return false, fmt.Errorf("Unrecognized optimization: %s", t.Text)
			}

			carryValue.Type = VAL_CLAUSE
			carryValue.Val = clause
		case ITOK_CMD_COMPILE:
			if carryValue.Type != VAL_CLAUSE {
				return false, fmt.Errorf("Unable to compile argument of type: %s", carryValue)
			}

			clause, ok := carryValue.Val.(*query.Clause)
			if !ok {
				return true, errors.New("Type corruption during compilation, expected *query.Clause")
			}

			query, params, err := clause.Compile()
			if err != nil {
				return false, err
			}

			fmt.Fprintf(w, "query:\n%s\n--------\nparams:\n%s\n", query, params)
			carryValue.Type = VAL_INVALID
			break out
		case ITOK_VAR_NAME:
			// NOTE: very brittle, only allows expansion of a single variable
			if i == len(tokens)-1 {
				carryValue, ok = inter.State[t.Text]
				if !ok {
					return false, fmt.Errorf("No variable: %s", t.Text)
				}
			} else {
				variableName = t.Text
			}
		case ITOK_VAL_STR:
			carryValue.Type = VAL_STRING
			carryValue.Val = t.Text
		case ITOK_VAL_INT:
			val, err := strconv.Atoi(t.Text)
			if err != nil {
				return false, fmt.Errorf("Unable to parse as integer: %v", err)
			}
			carryValue.Type = VAL_INT
			carryValue.Val = val
		case ITOK_CMD_LEN:
			var length int
			switch cType := carryValue.Type; cType {
			case VAL_STRING:
				s, ok := carryValue.Val.(string)
				if !ok {
					return true, fmt.Errorf("Type corruption during len, expected string")
				}
				length = len(s)
			case VAL_TOKENS:
				toks, ok := carryValue.Val.([]query.Token)
				if !ok {
					return true, fmt.Errorf("Type corruption during len, expected []query.Token")
				}
				length = len(toks)
			default:
				return false, fmt.Errorf("Unable to get length of argument with type: %s", carryValue.Type)
			}
			carryValue.Type = VAL_INT
			carryValue.Val = length
		case ITOK_CMD_SLICE:
			// TODO: get start and end of range
			switch cType := carryValue.Type; cType {
			case VAL_STRING:
			case VAL_TOKENS:
			default:
				return false, fmt.Errorf("Cannot slice argument: %v", cType)
			}
			return false, fmt.Errorf("not implemented")
		}
	}

	if carryValue.Type != VAL_INVALID {
		fmt.Fprintln(w, carryValue)
		inter.State["_"] = carryValue
	}

	return false, nil
}

func (inter Interpreter) Tokenize(line string) []IToken {
	var prevType ITokType
	tokens := make([]IToken, 0, 3)
	for word := range strings.SplitSeq(line, " ") {
		trimmedWord := strings.TrimSpace(word)
		if trimmedWord == "" {
			continue
		}

		if len(tokens) != 0 {
			prevType = tokens[len(tokens)-1].Type
		}

		if trimmedWord == "help" {
			tokens = append(tokens, IToken{Type: ITOK_CMD_HELP})
		} else if trimmedWord == "exit" {
			tokens = append(tokens, IToken{Type: ITOK_CMD_EXIT})
		} else if trimmedWord == "env" {
			tokens = append(tokens, IToken{Type: ITOK_CMD_ENV})
		} else if trimmedWord == "clear" {
			tokens = append(tokens, IToken{Type: ITOK_CMD_CLEAR})
		} else if trimmedWord == "let" {
			tokens = append(tokens, IToken{Type: ITOK_CMD_LET})
		} else if trimmedWord == "del" {
			tokens = append(tokens, IToken{Type: ITOK_CMD_DEL})
		} else if trimmedWord == "print" {
			tokens = append(tokens, IToken{Type: ITOK_CMD_PRINT})
		} else if trimmedWord == "len" {
			tokens = append(tokens, IToken{Type: ITOK_CMD_LEN})
		} else if trimmedWord == "slice" {
			tokens = append(tokens, IToken{Type: ITOK_CMD_SLICE})
		} else if trimmedWord == "rematch" {
			tokens = append(tokens, IToken{Type: ITOK_CMD_REMATCH})
		} else if trimmedWord == "repattern" {
			tokens = append(tokens, IToken{Type: ITOK_CMD_REPATTERN})
		} else if trimmedWord == "tokenize" {
			tokens = append(tokens, IToken{Type: ITOK_CMD_TOKENIZE})
		} else if trimmedWord == "parse" {
			tokens = append(tokens, IToken{Type: ITOK_CMD_PARSE})
		} else if l := len("env_"); len(trimmedWord) > l && trimmedWord[:l] == "env_" {
			tokens = append(tokens, IToken{ITOK_CMD_ENV, trimmedWord[l:]})
		} else if l := len("opt_"); len(trimmedWord) > l && trimmedWord[:l] == "opt_" {
			tokens = append(tokens, IToken{ITOK_CMD_OPTIMIZE, trimmedWord[l:]})
		} else if trimmedWord == "compile" {
			tokens = append(tokens, IToken{Type: ITOK_CMD_COMPILE})
		} else if prevType == ITOK_CMD_LET {
			tokens = append(tokens, IToken{ITOK_VAR_NAME, trimmedWord})
		} else if prevType == ITOK_CMD_DEL {
			tokens = append(tokens, IToken{ITOK_VAR_NAME, trimmedWord})
		} else if prevType == ITOK_CMD_PRINT {
			tokens = append(tokens, IToken{ITOK_VAR_NAME, trimmedWord})
		} else if prevType == ITOK_CMD_LEN || prevType == ITOK_CMD_SLICE {
			if trimmedWord[0] == '`' {
				_, strLiteral, _ := strings.Cut(word, "`")
				tokens = append(tokens, IToken{ITOK_VAL_STR, strLiteral})
			} else {
				tokens = append(tokens, IToken{ITOK_VAR_NAME, trimmedWord})
			}
		} else if prevType == ITOK_CMD_REMATCH || prevType == ITOK_CMD_TOKENIZE {
			if trimmedWord[0] == '`' {
				_, strLiteral, _ := strings.Cut(word, "`")
				tokens = append(tokens, IToken{ITOK_VAL_STR, strLiteral})
			} else {
				tokens = append(tokens, IToken{ITOK_VAR_NAME, trimmedWord})
			}
		} else if prevType == ITOK_CMD_PARSE || prevType == ITOK_CMD_OPTIMIZE || prevType == ITOK_CMD_COMPILE {
			tokens = append(tokens, IToken{ITOK_VAR_NAME, trimmedWord})
		} else if prevType == ITOK_VAR_NAME && trimmedWord[0] == '`' {
			_, strLiteral, _ := strings.Cut(word, "`")
			tokens = append(tokens, IToken{ITOK_VAL_STR, strLiteral})
		} else if prevType == ITOK_VAR_NAME && unicode.IsDigit(rune(trimmedWord[0])) {
			tokens = append(tokens, IToken{ITOK_VAL_INT, trimmedWord})
		} else if prevType == ITOK_VAL_STR {
			tokens[len(tokens)-1].Text += " " + word
		} else {
			tokens = append(tokens, IToken{ITOK_INVALID, trimmedWord})
		}
	}

	return tokens
}

func printHelp(w io.Writer) {
	fmt.Fprintln(w, "Shitty debug shell for atlas")
	fmt.Fprintln(w, "help                                  - print this help")
	fmt.Fprintln(w, "exit                                  - exit interactive mode")
	fmt.Fprintln(w, "env                                   - print info about environment")
	fmt.Fprintln(w, "    env_<name>                        - print about specific variable <name>")
	fmt.Fprintln(w, "clear                                 - clear the screen")
	fmt.Fprintln(w, "let name (string|tokens|clause)       - save value to a variable")
	fmt.Fprintln(w, "del [name]                            - delete a variable or all variables")
	fmt.Fprintln(w, "print [name]                          - print a variable or all variables")
	fmt.Fprintln(w, "slice (string|tokens|name) start stop - slice a string or tokens from start to stop")
	fmt.Fprintln(w, "len (string|tokens|name)              - length of a string or token slice")
	fmt.Fprintln(w, "rematch (string|name)                 - match against regex for querylang spec")
	fmt.Fprintln(w, "repattern                             - print regex for querylang")
	fmt.Fprintln(w, "tokenize (string|name)                - tokenize a string")
	fmt.Fprintln(w, "        ex. tokenize `author:me")
	fmt.Fprintln(w, "parse (tokens|name)                   - parse tokens into a clause")
	fmt.Fprintln(w, "opt_<subcommand> (clause|name)        - optimize clause tree")
	fmt.Fprintln(w, "    sort                              - sort statements")
	fmt.Fprintln(w, "    flatten                           - flatten clauses")
	fmt.Fprintln(w, "    compact                           - compact equivalent statements")
	fmt.Fprintln(w, "    tidy                              - remove zero statements and `AND` clauses containing any")
	fmt.Fprintln(w, "    contradictions                    - zero contradicting statements and clauses")
	fmt.Fprintln(w, "    strictEq                          - zero fuzzy/range statements when an eq is present")
	fmt.Fprintln(w, "    tighten                           - zero redundant fuzzy/range statements when another mathes the same values")
	fmt.Fprintln(w, "compile (clause|name)                 - compile clause into query")
	fmt.Fprintln(w, "\nBare commands which return a value assign to an implicit variable _")
}
