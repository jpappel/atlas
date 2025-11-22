package shell

import (
	"bytes"
	"fmt"
	"strings"

	"github.com/jpappel/atlas/pkg/index"
	"github.com/jpappel/atlas/pkg/query"
)

type ValueType int

const (
	VAL_INVALID ValueType = iota
	VAL_INT
	VAL_STRING
	VAL_TOKENS
	VAL_CLAUSE
	VAL_ARTIFACT
	VAL_RESULTS
)

type Value struct {
	Type ValueType
	Val  any
}

type State map[string]Value

func (t ValueType) String() string {
	switch t {
	case VAL_INVALID:
		return "Invalid"
	case VAL_INT:
		return "Integer"
	case VAL_STRING:
		return "String"
	case VAL_TOKENS:
		return "Tokens"
	case VAL_CLAUSE:
		return "Clause"
	case VAL_ARTIFACT:
		return "Compilation Artifact"
	case VAL_RESULTS:
		return "Query Result"
	default:
		return "Unkown"
	}
}

func (v Value) String() string {
	switch v.Type {
	case VAL_INT:
		i, ok := v.Val.(int)
		if !ok {
			panic("Corrupted Type (expected int)")
		}
		return fmt.Sprint(i)
	case VAL_STRING:
		s, ok := v.Val.(string)
		if !ok {
			panic("Corrupted Type (expected string)")
		}
		return s
	case VAL_TOKENS:
		ts, ok := v.Val.([]query.Token)
		if !ok {
			panic("Corrupted Type (expected []query.Token)")
		}
		return query.TokensStringify(ts)
	case VAL_CLAUSE:
		clause, ok := v.Val.(*query.Clause)
		if !ok {
			panic("Corrupted Type (expected *query.Clause)")
		}
		return clause.String()
	case VAL_ARTIFACT:
		artifact, ok := v.Val.(query.CompilationArtifact)
		if !ok {
			panic("Corrupted Type (expected query.CompilationArtifact)")
		}
		return artifact.String()
	case VAL_RESULTS:
		results, ok := v.Val.([]*index.Document)
		if !ok {
			panic("Corrupted Type (expected []*index.Document)")
		}

		b := bytes.Buffer{}
		yo := query.YamlOutput{}
		if _, err := yo.OutputTo(&b, results); err != nil {
			panic(err)
		}
		return b.String()
	case VAL_INVALID:
		return "Invalid"
	}
	return fmt.Sprintf("Unknown @ %p", v.Val)
}

func (s State) String() string {
	b := strings.Builder{}

	for k, v := range s {
		b.WriteString(k)
		b.WriteByte(':')
		b.WriteByte(' ')
		switch v.Type {
		case VAL_INVALID:
			b.WriteString("Invalid")
		case VAL_INT:
			b.WriteString("Integer")
		case VAL_STRING:
			b.WriteString("String")
		case VAL_TOKENS:
			b.WriteString("Tokens")
		case VAL_CLAUSE:
			b.WriteString("Clause")
		case VAL_ARTIFACT:
			b.WriteString("Artifact")
		case VAL_RESULTS:
			b.WriteString("Results")
		default:
			fmt.Fprintf(&b, "Unknown (%d)", v.Val)
		}
		b.WriteByte('\n')
	}

	return b.String()
}
