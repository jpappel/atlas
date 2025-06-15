package query_test

import (
	"errors"
	"slices"
	"testing"

	"github.com/jpappel/atlas/pkg/query"
)

const (
	CAT_UNKNOWN  = query.CAT_UNKNOWN
	CAT_TITLE    = query.CAT_TITLE
	CAT_AUTHOR   = query.CAT_AUTHOR
	CAT_DATE     = query.CAT_DATE
	CAT_FILETIME = query.CAT_FILETIME
	CAT_TAGS     = query.CAT_TAGS
	CAT_LINKS    = query.CAT_LINKS
	CAT_META     = query.CAT_META

	OP_UNKNOWN = query.OP_UNKNOWN
	OP_EQ      = query.OP_EQ
	OP_AP      = query.OP_AP
	OP_NE      = query.OP_NE
	OP_LT      = query.OP_LT
	OP_LE      = query.OP_LE
	OP_GE      = query.OP_GE
	OP_GT      = query.OP_GT
	OP_PIPE    = query.OP_PIPE
	OP_ARG     = query.OP_ARG
)

func TestParse(t *testing.T) {
	tests := []struct {
		name    string
		tokens  []query.Token
		want    *query.Clause
		wantErr error
	}{{
		"empty clause",
		[]query.Token{
			{Type: TOK_CLAUSE_START}, {Type: TOK_CLAUSE_AND}, {Type: TOK_CLAUSE_END},
		},
		&query.Clause{Operator: query.COP_AND},
		nil,
	}, {
		"simple clause",
		[]query.Token{
			{Type: TOK_CLAUSE_START}, {Type: TOK_CLAUSE_AND},
			{TOK_CAT_AUTHOR, "a"}, {TOK_OP_AP, ":"}, {TOK_VAL_STR, "ken thompson"},
			{Type: TOK_CLAUSE_END},
		},
		&query.Clause{
			Operator: query.COP_AND,
			Statements: []query.Statement{
				{Category: CAT_AUTHOR, Operator: OP_AP, Value: query.StringValue{"ken thompson"}},
			},
		},
		nil,
	}, {
		"nested clause",
		[]query.Token{
			{Type: TOK_CLAUSE_START}, {Type: TOK_CLAUSE_AND},
			{TOK_CAT_AUTHOR, "a"}, {TOK_OP_AP, ":"}, {TOK_VAL_STR, "Alonzo Church"},
			{Type: TOK_CLAUSE_START}, {Type: TOK_CLAUSE_OR},
			{TOK_CAT_AUTHOR, "a"}, {TOK_OP_EQ, "="}, {TOK_VAL_STR, "Alan Turing"},
			{Type: TOK_CLAUSE_END},
			{Type: TOK_CLAUSE_END},
		},
		&query.Clause{
			Operator: query.COP_AND,
			Statements: []query.Statement{
				{Category: CAT_AUTHOR, Operator: OP_AP, Value: query.StringValue{"Alonzo Church"}},
			},
			Clauses: []*query.Clause{
				{
					Operator: query.COP_OR,
					Statements: []query.Statement{
						{Category: CAT_AUTHOR, Operator: OP_EQ, Value: query.StringValue{"Alan Turing"}},
					},
				},
			},
		},
		nil,
	}}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotC, gotErr := query.Parse(tt.tokens)
			if !errors.Is(gotErr, tt.wantErr) {
				t.Fatalf("Different parse error than expected: got %v, want %v", gotErr, tt.wantErr)
			} else if gotErr != nil {
				return
			}

			got := slices.Collect(gotC.DFS())
			want := slices.Collect(tt.want.DFS())

			gotL, wantL := len(got), len(want)
			if gotL != wantL {
				t.Errorf("Different number of clauses than expected: got %d, want %d", gotL, wantL)
			}

			for i := range min(gotL, wantL) {
				gotC, wantC := got[i], want[i]

				if gotC.Operator != wantC.Operator {
					t.Error("Different clause operator than expected")
				} else if !slices.EqualFunc(gotC.Statements, wantC.Statements,
					func(s1, s2 query.Statement) bool {
						return s1.Negated == s2.Negated && s1.Category == s2.Category && s1.Operator == s2.Operator && s1.Value.Compare(s2.Value) == 0
					}) {
					t.Error("Different statements than expected")
				} else if len(gotC.Clauses) != len(wantC.Clauses) {
					t.Error("Different number of child clauses than expected")
				}

				if t.Failed() {
					t.Log("Got\n", gotC)
					t.Log("Want\n", wantC)
					break
				}
			}
		})
	}
}
