package shell

import (
	"errors"
	"fmt"
	"io"
	"slices"
	"strconv"
	"strings"
	"unicode"

	"github.com/jpappel/atlas/pkg/query"
	"github.com/jpappel/atlas/pkg/util"
	"golang.org/x/term"
)

const COMMENT_STR = "#"

type keywords struct {
	commands      []string
	variables     []string
	optimizations []string
}

type Interpreter struct {
	State    State
	Workers  uint
	env      map[string]string
	term     *term.Terminal
	keywords keywords
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
	ITOK_CMD_LVL_OPTIMIZE
	ITOK_CMD_OPTIMIZE
	ITOK_CMD_TOKENIZE
	ITOK_CMD_PARSE
	ITOK_CMD_COMPILE
)

type IToken struct {
	Type ITokType
	Text string
}

var optimizations = []string{
	"simplify",
	"tighten",
	"flatten",
	"sort",
	"tidy",
	"contradictions",
	"compact",
	"strictEq",
}

var commands = []string{
	"help",
	"clear",
	"let",
	"del",
	"slice",
	"rematch",
	"repattern",
	"tokenize",
	"parse",
	"compile",
	"optimize_",
}

func NewInterpreter(initialState State, env map[string]string, workers uint) *Interpreter {
	return &Interpreter{
		State: initialState,
		env:   env,
		keywords: keywords{
			commands:      commands,
			optimizations: optimizations,
		},
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
		b := &strings.Builder{}
		b.WriteString("Unexpected token(s)\n")
		for _, t := range tokens {
			if t.Type == ITOK_INVALID {
				b.WriteString(t.Text)
				suggestion, goodSuggestion := util.Nearest(
					t.Text,
					inter.keywords.commands,
					util.LevensteinDistance,
					5,
				)
				if goodSuggestion {
					fmt.Fprintf(b, ": Did you mean '%s'?", suggestion)
				}
				b.WriteByte('\n')
			}
		}
		return false, errors.New(b.String())
	}

	stack := make([]Value, 0, 5)
	var ok bool
out:
	for i := len(tokens) - 1; i >= 0; i-- {
		t := tokens[i]
		top := len(stack) - 1
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
					return false, fmt.Errorf("No env var %s", t.Text)
				}
				fmt.Fprintln(w, t.Text, ":", v)
			}
			break out
		case ITOK_CMD_LET:
			if top < 1 {
				return false, fmt.Errorf("Expected 2 args for let, recieved %d", len(stack))
			}

			name := stack[top]
			val := stack[top-1]
			stack = stack[:top-1]

			if name.Type != VAL_STRING {
				return false, fmt.Errorf("Unable to name variable using non-string %s", name.Type)
			} else if varName, ok := name.Val.(string); !ok {
				return true, fmt.Errorf("Type corruption during let, expected string")
			} else if varName == "" {
				return false, fmt.Errorf("Cannot use the empty string as a variable name")
			} else {
				if _, ok := inter.State[varName]; !ok {
					inter.keywords.variables = append(inter.keywords.variables, varName)
				}
				inter.State[varName] = val
			}

			break out
		case ITOK_CMD_DEL:
			if top < 0 {
				fmt.Fprintln(w, "Deleting all variables")
				inter.State = make(State)
				inter.keywords.variables = inter.keywords.variables[:0]
			} else {
				name := stack[top]
				stack = stack[:top]

				var varName string
				if name.Type != VAL_STRING {
					return false, fmt.Errorf("Cannot delete non-string named variable")
				} else if varName, ok = name.Val.(string); !ok {
					return true, fmt.Errorf("Type corruption during del, expected string")
				}

				idx := slices.Index(inter.keywords.variables, varName)
				if idx > 0 {
					inter.keywords.variables[idx] = inter.keywords.variables[len(inter.keywords.variables)-1]
					inter.keywords.variables = inter.keywords.variables[:len(inter.keywords.variables)-1]
				}
				delete(inter.State, varName)
			}
			break out
		case ITOK_CMD_PRINT:
			if top < 0 {
				fmt.Fprintln(w, "Variables:")
				fmt.Fprintln(w, inter.State)
			} else {
				name := stack[top]
				stack = stack[:top]

				if name.Type != VAL_STRING {
					return false, fmt.Errorf("Cannot print variable with non-string name")
				} else if varName, ok := name.Val.(string); !ok {
					return true, fmt.Errorf("Type corruption during print, expected string")
				} else if val, ok := inter.State[varName]; !ok {
					suggestion, ok := util.Nearest(
						varName,
						inter.keywords.variables,
						util.LevensteinDistance,
						3,
					)
					suggestionText := ""
					if ok {
						suggestionText = fmt.Sprintf(": Did you mean '%s'?", suggestion)
					}
					return false, fmt.Errorf("No variable %s%s", tokens[1].Text, suggestionText)
				} else {
					fmt.Fprintln(w, val)
				}
			}
			break out
		case ITOK_CMD_REMATCH:
			if top < 0 {
				return false, fmt.Errorf("No argument to match against")
			}
			arg := stack[top]
			stack = stack[:top]

			if arg.Type != VAL_STRING {
				return false, fmt.Errorf("Unable to match against argument of type %s", arg.Type)
			}

			body, ok := arg.Val.(string)
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
			stack = append(stack, Value{VAL_STRING, b.String()})
		case ITOK_CMD_REPATTERN:
			fmt.Fprintln(w, query.LexRegexPattern)
			break out
		case ITOK_CMD_TOKENIZE:
			if top < 0 {
				return false, fmt.Errorf("No argument provided to tokenize")
			}
			arg := stack[top]
			stack = stack[:top]

			if arg.Type != VAL_STRING {
				return false, fmt.Errorf("Unable to tokenize argument of type: %s", arg.Type)
			}

			rawQuery, ok := arg.Val.(string)
			if !ok {
				return true, errors.New("Type corruption during tokenize, expected string")
			}

			stack = append(stack, Value{VAL_TOKENS, query.Lex(rawQuery)})
		case ITOK_CMD_PARSE:
			if top < 0 {
				return false, fmt.Errorf("No argument to parse")
			}
			arg := stack[top]
			stack = stack[:top]

			if arg.Type != VAL_TOKENS {
				return false, fmt.Errorf("Unable to parse argument of type: %s", arg.Type)
			}

			queryTokens, ok := arg.Val.([]query.Token)
			if !ok {
				return true, errors.New("Type corruption during parse, expected []query.Tokens")
			}

			clause, err := query.Parse(queryTokens)
			if err != nil {
				return false, err
			}

			stack = append(stack, Value{VAL_CLAUSE, clause})
		case ITOK_CMD_LVL_OPTIMIZE:
			if top < 0 {
				return false, fmt.Errorf("No argument to leveled optimize")
			}
			arg := stack[top]
			stack = stack[:top]

			if arg.Type != VAL_CLAUSE {
				return false, fmt.Errorf("Unable to optimize argument of type: %s", arg.Type)
			}

			level, err := strconv.Atoi(t.Text)
			if err != nil {
				return false, fmt.Errorf("Cannot parse optimization level %s", t.Text)
			}

			clause, ok := arg.Val.(*query.Clause)
			if !ok {
				return true, errors.New("Type corruption during optimization, expected *query.Clause")
			}
			o := query.NewOptimizer(clause, inter.Workers)
			o.Optimize(level)

			stack = append(stack, Value{VAL_CLAUSE, clause})
		case ITOK_CMD_OPTIMIZE:
			if top < 0 {
				return false, fmt.Errorf("No argument to optimize")
			}
			arg := stack[top]
			stack = stack[:top]

			if arg.Type != VAL_CLAUSE {
				return false, fmt.Errorf("Unable to optimize argument of type: %s", arg.Type)
			}

			clause, ok := arg.Val.(*query.Clause)
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
				suggestion, ok := util.Nearest(
					t.Text,
					inter.keywords.optimizations,
					util.LevensteinDistance,
					4,
				)
				suggestionTxt := ""
				if ok {
					suggestionTxt = fmt.Sprintf(": Did you mean '%s'?", suggestion)
				}
				return false, fmt.Errorf("Unrecognized optimization %s%s", t.Text, suggestionTxt)
			}

			stack = append(stack, Value{VAL_CLAUSE, clause})
		case ITOK_CMD_COMPILE:
			if top < 0 {
				return false, fmt.Errorf("No argument to compile")
			}
			arg := stack[top]
			stack = stack[:top]

			if arg.Type != VAL_CLAUSE {
				return false, fmt.Errorf("Unable to compile argument of type %s", arg.Type)
			}

			clause, ok := arg.Val.(*query.Clause)
			if !ok {
				return true, errors.New("Type corruption during compilation, expected *query.Clause")
			}

			artifact, err := clause.Compile()
			if err != nil {
				return false, err
			}

			stack = append(stack, Value{VAL_ARTIFACT, artifact})
		case ITOK_VAR_NAME:
			val, ok := inter.State[t.Text]
			if !ok {
				suggestion, ok := util.Nearest(
					t.Text,
					inter.keywords.variables,
					util.LevensteinDistance,
					4,
				)
				suggestionTxt := ""
				if ok {
					suggestionTxt = fmt.Sprintf(": Did you mean '%s'?", suggestion)
				}
				return false, fmt.Errorf("No variable %s%s", t.Text, suggestionTxt)
			}
			stack = append(stack, val)
		case ITOK_VAL_STR:
			stack = append(stack, Value{VAL_STRING, t.Text})
		case ITOK_VAL_INT:
			val, err := strconv.Atoi(t.Text)
			if err != nil {
				return false, fmt.Errorf("Unable to parse as integer %v", err)
			}
			stack = append(stack, Value{VAL_INT, val})
		case ITOK_CMD_LEN:
			if top < 0 {
				return false, fmt.Errorf("No argument to get the length of")
			}
			arg := stack[top]
			stack = stack[:top]

			var length int
			switch cType := arg.Type; cType {
			case VAL_STRING:
				s, ok := arg.Val.(string)
				if !ok {
					return true, fmt.Errorf("Type corruption during len, expected string")
				}
				length = len(s)
			case VAL_TOKENS:
				toks, ok := arg.Val.([]query.Token)
				if !ok {
					return true, fmt.Errorf("Type corruption during len, expected []query.Token")
				}
				length = len(toks)
			default:
				return false, fmt.Errorf("Unable to get length of argument with type %s", arg.Type)
			}

			stack = append(stack, Value{VAL_INT, length})
		case ITOK_CMD_SLICE:
			if top < 2 {
				return false, fmt.Errorf("Expected 3 arguments for slice, got %d", len(stack))
			}
			arg := stack[top]
			start := stack[top-1]
			stop := stack[top-2]
			stack = stack[:top-2]

			var startIdx, stopIdx int
			if start.Type != VAL_INT {
				return false, fmt.Errorf("Expected integer to start slicing, got %s", start.Type)
			} else if stop.Type != VAL_INT {
				return false, fmt.Errorf("Expected integer to stop slicing, got %s", stop.Type)
			} else {
				startIdx, ok = start.Val.(int)
				if !ok {
					return true, fmt.Errorf("Type corruption during slice, expected integer")
				}
				stopIdx, ok = stop.Val.(int)
				if !ok {
					return true, fmt.Errorf("Type corruption during slice, expected integer")
				}
			}

			switch arg.Type {
			case VAL_STRING:
				s, ok := arg.Val.(string)
				if !ok {
					return true, fmt.Errorf("Type corruption during slice, expected string")
				}

				if 0 <= startIdx && startIdx <= stopIdx && stopIdx <= len(s) {
					stack = append(stack, Value{VAL_STRING, s[startIdx:stopIdx]})
				} else {
					return false, fmt.Errorf(
						"Indexes [%d:%d] out of range [0:%d]",
						startIdx, stopIdx, len(s),
					)
				}
			case VAL_TOKENS:
				qTokens, ok := arg.Val.([]query.Token)
				if !ok {
					return true, fmt.Errorf("Type corruption during slice, expected []query.Token")
				}

				if 0 <= startIdx && startIdx <= stopIdx && stopIdx <= len(qTokens) {
					stack = append(stack, Value{VAL_TOKENS, qTokens[startIdx:stopIdx]})
				} else {
					return false, fmt.Errorf(
						"Indexes [%d:%d] out of range [0:%d]",
						startIdx, stopIdx, len(qTokens),
					)
				}
			default:
				return false, fmt.Errorf("Cannot slice argument of type %s", arg.Type)
			}
		}
	}

	for _, e := range stack {
		fmt.Fprintln(w, e)
	}
	if len(stack) > 0 {
		inter.State["_"] = stack[len(stack)-1]
	}

	return false, nil
}

func (inter Interpreter) Tokenize(line string) []IToken {
	var prevType ITokType
	tokens := make([]IToken, 0, 3)

	if line[:len(COMMENT_STR)] == COMMENT_STR {
		return tokens
	}

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
		} else if l := len("optimize_"); len(trimmedWord) > l && trimmedWord[:l] == "optimize_" {
			tokens = append(tokens, IToken{ITOK_CMD_LVL_OPTIMIZE, trimmedWord[l:]})
		} else if l := len("env_"); len(trimmedWord) > l && trimmedWord[:l] == "env_" {
			tokens = append(tokens, IToken{ITOK_CMD_ENV, trimmedWord[l:]})
		} else if l := len("opt_"); len(trimmedWord) > l && trimmedWord[:l] == "opt_" {
			tokens = append(tokens, IToken{ITOK_CMD_OPTIMIZE, trimmedWord[l:]})
		} else if trimmedWord == "compile" {
			tokens = append(tokens, IToken{Type: ITOK_CMD_COMPILE})
		} else if unicode.IsDigit(rune(trimmedWord[0])) {
			tokens = append(tokens, IToken{ITOK_VAL_INT, trimmedWord})
		} else if prevType == ITOK_CMD_LET || prevType == ITOK_CMD_DEL || prevType == ITOK_CMD_PRINT {
			tokens = append(tokens, IToken{ITOK_VAL_STR, trimmedWord})
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
		} else if prevType == ITOK_CMD_PARSE || prevType == ITOK_CMD_OPTIMIZE || prevType == ITOK_CMD_LVL_OPTIMIZE || prevType == ITOK_CMD_COMPILE {
			tokens = append(tokens, IToken{ITOK_VAR_NAME, trimmedWord})
		} else if prevType == ITOK_VAL_STR && len(tokens) > 1 && tokens[len(tokens)-2].Type == ITOK_CMD_LET && trimmedWord[0] == '`' {
			_, strLiteral, _ := strings.Cut(word, "`")
			tokens = append(tokens, IToken{ITOK_VAL_STR, strLiteral})
		} else if prevType == ITOK_VAL_STR && len(tokens) > 1 && tokens[len(tokens)-2].Type != ITOK_CMD_LET {
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
	fmt.Fprintln(w, "optimize_<level> (clause|name)        - optimize clause tree to <level>")
	fmt.Fprintln(w, "opt_<subcommand> (clause|name)        - apply specific optimization to clause tree")
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

func init() {
	for _, opt := range optimizations {
		commands = append(commands, "opt_"+opt)
	}
}
