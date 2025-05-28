package query

import (
	"testing"
)

func TestLex(t *testing.T) {
	tests := []struct {
		name  string
		query string
		want  []Token
	}{
		{"empty query", "", []Token{{Type: TOK_CLAUSE_START}, {TOK_CLAUSE_AND, "and"}, {Type: TOK_CLAUSE_END}}},
		{"quoted statement", `a:"ken thompson"`, []Token{
			{Type: TOK_CLAUSE_START}, {TOK_CLAUSE_AND, "and"},
			{TOK_CAT_AUTHOR, "a"}, {TOK_OP_AP, ":"}, {TOK_VAL_STR, "ken thompson"},
			{Type: TOK_CLAUSE_END},
		}},
		{"invalid token", `foo:bar`, []Token{
			{Type: TOK_CLAUSE_START}, {TOK_CLAUSE_AND, "and"},
			{TOK_UNKNOWN, "foo:bar"},
			{Type: TOK_CLAUSE_END},
		}},
		{"simple query", "a:a t:b d:01010001", []Token{
			{Type: TOK_CLAUSE_START}, {TOK_CLAUSE_AND, "and"},
			{TOK_CAT_AUTHOR, "a"}, {TOK_OP_AP, ":"}, {TOK_VAL_STR, "a"},
			{TOK_CAT_TAGS, "t"}, {TOK_OP_AP, ":"}, {TOK_VAL_STR, "b"},
			{TOK_CAT_DATE, "d"}, {TOK_OP_AP, ":"}, {TOK_VAL_DATETIME, "01010001"},
			{Type: TOK_CLAUSE_END},
		}},
		{"nested clauses", "a:a (or t:b t!=c) or d<=01010001 and -T~foo", []Token{
			{Type: TOK_CLAUSE_START}, {TOK_CLAUSE_AND, "and"},
			{TOK_CAT_AUTHOR, "a"}, {TOK_OP_AP, ":"}, {TOK_VAL_STR, "a"},
			{Type: TOK_CLAUSE_START}, {TOK_CLAUSE_OR, "or"},
			{TOK_CAT_TAGS, "t"}, {TOK_OP_AP, ":"}, {TOK_VAL_STR, "b"},
			{TOK_CAT_TAGS, "t"}, {TOK_OP_NE, "!="}, {TOK_VAL_STR, "c"},
			{Type: TOK_CLAUSE_END},
			{Type: TOK_CLAUSE_START}, {TOK_CLAUSE_OR, "or"},
			{TOK_CAT_DATE, "d"}, {TOK_OP_LE, "<="}, {TOK_VAL_DATETIME, "01010001"},
			{Type: TOK_CLAUSE_START}, {TOK_CLAUSE_AND, "and"},
			{TOK_OP_NEG, "-"}, {TOK_CAT_TITLE, "T"}, {TOK_OP_AP, "~"}, {TOK_VAL_STR, "foo"},
			{Type: TOK_CLAUSE_END},
			{Type: TOK_CLAUSE_END},
			{Type: TOK_CLAUSE_END},
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := Lex(tt.query)

			gl, wl := len(got), len(tt.want)
			if gl != wl {
				t.Errorf("Got %d tokens wanted %d\n", gl, wl)
			}

			for i := range min(gl, wl) {
				gt, wt := got[i], tt.want[i]
				if !gt.Equal(wt) {
					t.Errorf("Got different token than wanted at %d\n", i)
					t.Logf("(%v) != (%v)\n", gt.String(), wt.String())
					break
				}
			}

			if t.Failed() {
				t.Log("Got\n", treeStringify(got))
				t.Log("Want\n", treeStringify(tt.want))
			}
		})
	}
}
