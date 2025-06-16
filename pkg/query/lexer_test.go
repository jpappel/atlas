package query_test

import (
	"testing"

	"github.com/jpappel/atlas/pkg/query"
)

type Token = query.Token

const (
	TOK_UNKNOWN      = query.TOK_UNKNOWN
	TOK_CLAUSE_OR    = query.TOK_CLAUSE_OR
	TOK_CLAUSE_AND   = query.TOK_CLAUSE_AND
	TOK_CLAUSE_START = query.TOK_CLAUSE_START
	TOK_CLAUSE_END   = query.TOK_CLAUSE_END
	TOK_OP_NEG       = query.TOK_OP_NEG
	TOK_OP_EQ        = query.TOK_OP_EQ
	TOK_OP_AP        = query.TOK_OP_AP
	TOK_OP_NE        = query.TOK_OP_NE
	TOK_OP_LT        = query.TOK_OP_LT
	TOK_OP_LE        = query.TOK_OP_LE
	TOK_OP_GE        = query.TOK_OP_GE
	TOK_OP_GT        = query.TOK_OP_GT
	TOK_OP_PIPE      = query.TOK_OP_PIPE
	TOK_OP_ARG       = query.TOK_OP_ARG
	TOK_CAT_TITLE    = query.TOK_CAT_TITLE
	TOK_CAT_AUTHOR   = query.TOK_CAT_AUTHOR
	TOK_CAT_DATE     = query.TOK_CAT_DATE
	TOK_CAT_FILETIME = query.TOK_CAT_FILETIME
	TOK_CAT_TAGS     = query.TOK_CAT_TAGS
	TOK_CAT_LINKS    = query.TOK_CAT_LINKS
	TOK_CAT_META     = query.TOK_CAT_META
	TOK_VAL_STR      = query.TOK_VAL_STR
	TOK_VAL_DATETIME = query.TOK_VAL_DATETIME
)

func TestLex(t *testing.T) {
	tests := []struct {
		name  string
		query string
		want  []query.Token
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
		{"leading subclause", "(or a:a a:b)", []Token{
			{Type: TOK_CLAUSE_START}, {TOK_CLAUSE_AND, "and"},
			{Type: TOK_CLAUSE_START}, {TOK_CLAUSE_OR, "or"},
			{TOK_CAT_AUTHOR, "a"}, {TOK_OP_AP, ":"}, {TOK_VAL_STR, "a"},
			{TOK_CAT_AUTHOR, "a"}, {TOK_OP_AP, ":"}, {TOK_VAL_STR, "b"},
			{Type: TOK_CLAUSE_END},
			{Type: TOK_CLAUSE_END},
		}},
		{"clause after clause", "(or a:a a:b) (or a:c a:d)", []Token{
			{Type: TOK_CLAUSE_START}, {TOK_CLAUSE_AND, "and"},
			{Type: TOK_CLAUSE_START}, {TOK_CLAUSE_OR, "or"},
			{TOK_CAT_AUTHOR, "a"}, {TOK_OP_AP, ":"}, {TOK_VAL_STR, "a"},
			{TOK_CAT_AUTHOR, "a"}, {TOK_OP_AP, ":"}, {TOK_VAL_STR, "b"},
			{Type: TOK_CLAUSE_END},
			{Type: TOK_CLAUSE_START}, {TOK_CLAUSE_OR, "or"},
			{TOK_CAT_AUTHOR, "a"}, {TOK_OP_AP, ":"}, {TOK_VAL_STR, "c"},
			{TOK_CAT_AUTHOR, "a"}, {TOK_OP_AP, ":"}, {TOK_VAL_STR, "d"},
			{Type: TOK_CLAUSE_END},
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
		{"consecutive clause starts", "a:a (or (and a:b a:c) a:d)", []Token{
			{Type: TOK_CLAUSE_START}, {TOK_CLAUSE_AND, "and"},
			{TOK_CAT_AUTHOR, "a"}, {TOK_OP_AP, ":"}, {TOK_VAL_STR, "a"},
			{Type: TOK_CLAUSE_START}, {TOK_CLAUSE_OR, "or"},
			{Type: TOK_CLAUSE_START}, {TOK_CLAUSE_AND, "and"},
			{TOK_CAT_AUTHOR, "a"}, {TOK_OP_AP, ":"}, {TOK_VAL_STR, "b"},
			{TOK_CAT_AUTHOR, "a"}, {TOK_OP_AP, ":"}, {TOK_VAL_STR, "c"},
			{Type: TOK_CLAUSE_END},
			{TOK_CAT_AUTHOR, "a"}, {TOK_OP_AP, ":"}, {TOK_VAL_STR, "d"},
			{Type: TOK_CLAUSE_END},
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := query.Lex(tt.query)

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
				t.Log("Got\n", query.TokensStringify(got))
				t.Log("Want\n", query.TokensStringify(tt.want))
			}
		})
	}
}
