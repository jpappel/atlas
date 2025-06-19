package shell

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"os"
	"os/signal"
	"slices"
	"strconv"
	"strings"
	"syscall"
	"unicode"

	"github.com/jpappel/atlas/pkg/query"
)

type Interpreter struct {
	State   State
	Scanner *bufio.Scanner
	Workers uint
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
)

type IToken struct {
	Type ITokType
	Text string
}

func NewInterpreter(initialState State, inputSource io.Reader, workers uint) *Interpreter {
	return &Interpreter{
		initialState,
		bufio.NewScanner(inputSource),
		workers,
	}
}

func (interpreter *Interpreter) Reset() {
	interpreter.State = make(State)
}

func (interpreter *Interpreter) Eval(tokens []IToken) (bool, error) {
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
	for i := len(tokens) - 1; i >= 0; i-- {
		t := tokens[i]
		switch t.Type {
		case ITOK_CMD_HELP:
			printHelp()
			break
		case ITOK_CMD_CLEAR:
			fmt.Println("\033[H\033[J")
			break
		case ITOK_CMD_LET:
			if variableName != "" {
				interpreter.State[variableName] = carryValue
				carryValue.Type = VAL_INVALID
			}
			break
		case ITOK_CMD_DEL:
			if len(tokens) == 1 {
				fmt.Println("Deleting all variables")
				interpreter.State = make(State)
			} else {
				// HACK: variable name is not evaluated correctly so just look at the next token
				delete(interpreter.State, tokens[i+1].Text)
			}
			carryValue.Type = VAL_INVALID
			break
		case ITOK_CMD_PRINT:
			if len(tokens) == 1 {
				fmt.Println("Variables:")
				fmt.Println(interpreter.State)
			} else {
				carryValue, ok = interpreter.State[tokens[1].Text]
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
			fmt.Println(query.LexRegexPattern)
			break
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
				return false, fmt.Errorf("Unable to flatten argument of type: %s", carryValue)
			}

			clause, ok := carryValue.Val.(*query.Clause)
			if !ok {
				return true, errors.New("Type corruption during flatten, expected *query.Clause")
			}

			o := query.NewOptimizer(clause, interpreter.Workers)
			switch t.Text {
			case "simplify":
				o.Simplify()
			case "tighten":
				o.Tighten()
			case "flatten":
				o.Flatten()
			case "sortStatements":
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
		case ITOK_VAR_NAME:
			// NOTE: very brittle, only allows expansion of a single variable
			if i == len(tokens)-1 {
				carryValue, ok = interpreter.State[t.Text]
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
			fmt.Println("not implemented yet ;)")
			break
		}
	}

	if carryValue.Type != VAL_INVALID {
		fmt.Println(carryValue)
		interpreter.State["_"] = carryValue
	}

	return false, nil
}

func (interpreter Interpreter) Tokenize(line string) []IToken {
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
		} else if l := len("opt_"); len(trimmedWord) > l && trimmedWord[:l] == "opt_" {
			tokens = append(tokens, IToken{ITOK_CMD_OPTIMIZE, trimmedWord[l:]})
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
		} else if prevType == ITOK_CMD_PARSE {
			tokens = append(tokens, IToken{ITOK_VAR_NAME, trimmedWord})
		} else if prevType == ITOK_CMD_OPTIMIZE {
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

func (interpreter Interpreter) Run() error {
	signalCh := make(chan os.Signal, 1)
	exitCh := make(chan error, 1)
	lineCh := make(chan string)
	defer close(signalCh)
	defer close(lineCh)
	defer close(exitCh)

	signal.Notify(signalCh, syscall.SIGINT)
	go func(output chan<- string, exitCh chan<- error) {
		for {
			if interpreter.Scanner.Scan() {
				output <- interpreter.Scanner.Text()
			} else if err := interpreter.Scanner.Err(); err != nil {
				exitCh <- err
				return
			} else {
				exitCh <- io.EOF
				return
			}
		}
	}(lineCh, exitCh)

	for {
		fmt.Print("atlasi> ")

		select {
		case <-signalCh:
			fmt.Println("Recieved Ctrl-C, exitting")
			return nil
		case err := <-exitCh:
			return err
		case line := <-lineCh:
			tokens := interpreter.Tokenize(line)
			fatal, err := interpreter.Eval(tokens)
			if fatal {
				return err
			} else if err != nil {
				fmt.Println(err)
			}
		}
	}
}

func printHelp() {
	fmt.Println("Shitty debug shell for atlas")
	fmt.Println("help                                  - print this help")
	fmt.Println("clear                                 - clear the screen")
	fmt.Println("let name (string|tokens|clause)       - save value to a variable")
	fmt.Println("del [name]                            - delete a variable or all variables")
	fmt.Println("print [name]                          - print a variable or all variables")
	fmt.Println("slice (string|tokens|name) start stop - slice a string or tokens from start to stop")
	fmt.Println("len (string|tokens|name)              - length of a string or token slice")
	fmt.Println("rematch (string|name)                 - match against regex for querylang spec")
	fmt.Println("repattern                             - print regex for querylang")
	fmt.Println("tokenize (string|name)                - tokenize a string")
	fmt.Println("        ex. tokenize `author:me")
	fmt.Println("parse (tokens|name)                   - parse tokens into a clause")
	fmt.Println("opt_<subcommand> (clause|name)   - optimize clause tree")
	fmt.Println("    sortStatements               - sort statements")
	fmt.Println("    flatten                      - flatten clauses")
	fmt.Println("    compact                      - compact equivalent statements")
	fmt.Println("    tidy                         - remove zero statements and `AND` clauses containing any")
	fmt.Println("    contradictions               - zero contradicting statements and clauses")
	fmt.Println("    strictEq                     - zero fuzzy/range statements when an eq is present")
	fmt.Println("    tighten                      - zero redundant fuzzy/range statements when another mathes the same values")
	fmt.Println("\nBare commands which return a value assign to an implicit variable _")
}
