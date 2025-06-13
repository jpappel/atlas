package shell

import (
	"errors"
	"fmt"
	"os"
	"strings"

	"github.com/jpappel/atlas/pkg/query"
)

type ValueType int

const (
	INVALID ValueType = iota
	STRING
	TOKENS
	CLAUSE
)

type Value struct {
	Type ValueType
	Val  any
}

type State map[string]Value

func (v Value) String() string {
	switch v.Type {
	case STRING:
		s, ok := v.Val.(string)
		if !ok {
			return "Corrupted Type (expected string)"
		}
		return s
	case TOKENS:
		ts, ok := v.Val.([]query.Token)
		if !ok {
			return "Corrupted Type (expected []query.Token)"
		}
		return query.TokensStringify(ts)
	case CLAUSE:
		rootClause, ok := v.Val.(*query.Clause)
		if !ok {
			return "Corrupted Type (expected query.Clause)"
		}
		return rootClause.String()
	case INVALID:
		return "Invalid"
	}
	return fmt.Sprintf("Unknown @ %p", v.Val)
}

func (s State) String() string {
	b := strings.Builder{}

	for k, v := range s {
		b.WriteString(k)
		b.WriteByte(':')
		switch v.Type {
		case INVALID:
			b.WriteString(" Invalid")
		case STRING:
			b.WriteString(" String")
		case TOKENS:
			b.WriteString(" Tokens")
		case CLAUSE:
			b.WriteString(" Clause")
		default:
			fmt.Fprintf(&b, " Unknown (%d)", v.Val)
		}
		b.WriteByte('\n')
	}

	return b.String()
}

func (s State) CmdTokenize(input string) (Value, bool) {
	if len(input) == 0 {
		return Value{}, false
	}

	var rawQuery string
	if input[0] == '`' {
		rawQuery = input[1:]
	} else {
		variable, ok := s[input]
		if !ok {
			fmt.Fprintln(os.Stderr, "Cannot tokenize: no variable with name", input)
			return Value{}, false
		} else if variable.Type != STRING {
			fmt.Fprintln(os.Stderr, "Cannot tokenize: variable is not a string")
			return Value{}, false
		}

		rawQuery, ok = variable.Val.(string)
		if !ok {
			fmt.Fprintln(os.Stderr, "Cannot tokenize: type corruption")
			fmt.Fprintln(os.Stderr, "Type corruption, expected string")
			panic("Type corruption")
		}
	}
	tokens := query.Lex(rawQuery)
	return Value{TOKENS, tokens}, true
}

func (s State) CmdParse(args string) (Value, error) {
	if len(args) == 0 {
		return Value{}, errors.New("no arguments for parse")
	}

	var tokens []query.Token
	if tokenizeArgs, found := strings.CutPrefix(args, "tokenize "); found {
		val, ok := s.CmdTokenize(tokenizeArgs)
		if !ok {
			return Value{}, errors.New("error occured during tokenization")
		}
		tokens = val.Val.([]query.Token)
	} else {
		variable, ok := s[args]
		if !ok {
			fmt.Fprintln(os.Stderr, "Cannot parse: no variable with name", args)
			return Value{}, errors.New("variable does not exist")
		} else if variable.Type != TOKENS {
			fmt.Fprintln(os.Stderr, "Cannot parse: variable is not []query.Tokens")
			return Value{}, errors.New("bad variable type")
		}

		tokens, ok = variable.Val.([]query.Token)
		if !ok {
			fmt.Fprintln(os.Stderr, "Cannot parse: type corruption")
			fmt.Fprintln(os.Stderr, "Type corruption, expected []query.Tokens")
			panic("Type corruption")
		}
	}

	clause, err := query.Parse(tokens)
	if err != nil {
		return Value{}, err
	}
	return Value{CLAUSE, *clause}, err
}
