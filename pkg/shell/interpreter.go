package shell

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"os"
	"os/signal"
	"slices"
	"strings"
	"syscall"

	"github.com/jpappel/atlas/pkg/query"
)

type Interpreter struct {
	State   State
	Scanner *bufio.Scanner
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
	ITOK_CMD_LET
	ITOK_CMD_DEL
	ITOK_CMD_PRINT
	ITOK_CMD_LEN
	ITOK_CMD_SLICE
	ITOK_CMD_REMATCH
	ITOK_CMD_REPATTERN
	ITOK_CMD_FLATTEN
	ITOK_CMD_TOKENIZE
	ITOK_CMD_PARSE
)

type IToken struct {
	Type ITokType
	Text string
}

func NewInterpreter(initialState State, inputSource io.Reader) *Interpreter {
	return &Interpreter{
		initialState,
		bufio.NewScanner(inputSource),
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
		case ITOK_CMD_LET:
			interpreter.State[variableName] = carryValue
			carryValue.Type = INVALID
			break
		case ITOK_CMD_DEL:
			if len(tokens) == 1 {
				fmt.Println("Deleting all variables")
				interpreter.State = make(State)
			} else {
				// HACK: variable name is not evaluated correctly so just look at the next token
				delete(interpreter.State, tokens[i+1].Text)
			}
			carryValue.Type = INVALID
			break
		case ITOK_CMD_PRINT:
			if len(tokens) == 1 {
				fmt.Println("Variables:")
				fmt.Println(interpreter.State)
			} else {
				carryValue, ok = interpreter.State[tokens[1].Text]
				if !ok {
					return false, errors.New("No variable found with name " + tokens[1].Text)
				}
			}
		case ITOK_CMD_REMATCH:
			if carryValue.Type != STRING {
				return false, errors.New("Unable to match against argument")
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
			if carryValue.Type != STRING {
				return false, errors.New("Unable to tokenize argument")
			}

			rawQuery, ok := carryValue.Val.(string)
			if !ok {
				return true, errors.New("Type corruption during tokenize, expected string")
			}
			carryValue.Type = TOKENS
			carryValue.Val = query.Lex(rawQuery)
		case ITOK_CMD_PARSE:
			if carryValue.Type != TOKENS {
				fmt.Println("Carry type: ", carryValue.Type)
				return false, errors.New("Unable to parse argument")
			}

			queryTokens, ok := carryValue.Val.([]query.Token)
			if !ok {
				return true, errors.New("Type corruption during parse, expected []query.Tokens")
			}

			clause, err := query.Parse(queryTokens)
			if err != nil {
				return false, err
			}
			carryValue.Type = CLAUSE
			carryValue.Val = clause
		case ITOK_CMD_FLATTEN:
			if carryValue.Type != CLAUSE {
				fmt.Println("Carry type: ", carryValue.Type)
				return false, errors.New("Unable to parse argument")
			}

			clause, ok := carryValue.Val.(*query.Clause)
			if !ok {
				return true, errors.New("Type corruption during parse, expected []query.Tokens")
			}

			clause.Flatten()
			carryValue.Type = CLAUSE
			carryValue.Val = clause
		case ITOK_VAR_NAME:
			// NOTE: very brittle, only allows expansion of a single variable
			if i == len(tokens)-1 {
				carryValue, ok = interpreter.State[t.Text]
				if !ok {
					return false, errors.New("No variable: " + t.Text)
				}
			} else {
				variableName = t.Text
			}
		case ITOK_VAL_STR:
			carryValue.Type = STRING
			carryValue.Val = t.Text
		case ITOK_CMD_LEN:
			fmt.Println("not implemented yet ;)")
			break
		case ITOK_CMD_SLICE:
			fmt.Println("not implemented yet ;)")
			break
		}
	}

	if carryValue.Type != INVALID {
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
		} else if trimmedWord == "flatten" {
			tokens = append(tokens, IToken{Type: ITOK_CMD_FLATTEN})
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
		} else if prevType == ITOK_CMD_FLATTEN {
			tokens = append(tokens, IToken{ITOK_VAR_NAME, trimmedWord})
		} else if prevType == ITOK_VAR_NAME && trimmedWord[0] == '`' {
			_, strLiteral, _ := strings.Cut(word, "`")
			tokens = append(tokens, IToken{ITOK_VAL_STR, strLiteral})
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
		fmt.Print("> ")

		select {
		case <-signalCh:
			// TODO: log info to output
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
	fmt.Println("Commands: help, let, del, print, tokenize, parse")
	fmt.Println("help                                  - print this help")
	fmt.Println("let name (string|tokens|clause)       - save value to a variable")
	fmt.Println("del [name]                            - delete a variable or all variables")
	fmt.Println("print [name]                          - print a variable or all variables")
	fmt.Println("slice (string|tokens|name) start stop - slice a string or tokens from start to stop")
	fmt.Println("rematch (string|name)                 - match against regex for querylang spec")
	fmt.Println("repattern                             - print regex for querylang")
	fmt.Println("tokenize (string|name)                - tokenize a string")
	fmt.Println("        ex. tokenize `author:me")
	fmt.Println("parse (tokens|name)                   - parse tokens into a clause")
	fmt.Println("flatten (clause|name)                 - flatten a clause")
	fmt.Println("\nBare commands which return a value assign to an implicit variable _")
}
