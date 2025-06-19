package query_test

import (
	"runtime"
	"slices"
	"testing"

	"github.com/jpappel/atlas/pkg/query"
)

func TestClause_Flatten(t *testing.T) {
	tests := []struct {
		name     string
		root     *query.Clause
		expected query.Clause
	}{
		{
			"empty",
			&query.Clause{},
			query.Clause{},
		},
		{
			"empty with child",
			&query.Clause{
				Operator: query.COP_OR,
				Clauses: []*query.Clause{
					{
						Operator: query.COP_AND,
						Statements: []query.Statement{
							{Category: query.CAT_AUTHOR, Operator: query.OP_AP, Value: query.StringValue{"jp"}},
						},
					},
				},
			},
			query.Clause{
				Operator: query.COP_AND,
				Statements: []query.Statement{
					{Category: query.CAT_AUTHOR, Operator: query.OP_AP, Value: query.StringValue{"jp"}},
				},
			},
		},
		{
			"already flat",
			&query.Clause{
				Operator: query.COP_AND,
				Statements: []query.Statement{
					{Category: query.CAT_AUTHOR, Operator: query.OP_AP, Value: query.StringValue{"jp"}},
					{Category: query.CAT_TAGS, Operator: query.OP_EQ, Value: query.StringValue{"foobar"}},
					{Category: query.CAT_TITLE, Operator: query.OP_AP, Value: query.StringValue{"a very interesting title"}},
				},
			},
			query.Clause{
				Operator: query.COP_AND,
				Statements: []query.Statement{
					{Category: query.CAT_TITLE, Operator: query.OP_AP, Value: query.StringValue{"a very interesting title"}},
					{Category: query.CAT_TAGS, Operator: query.OP_EQ, Value: query.StringValue{"foobar"}},
					{Category: query.CAT_AUTHOR, Operator: query.OP_AP, Value: query.StringValue{"jp"}},
				},
			},
		},
		{
			"flatten 1 layer, multiple clauses",
			&query.Clause{
				Operator: query.COP_OR,
				Statements: []query.Statement{
					{Category: query.CAT_AUTHOR, Operator: query.OP_AP, Value: query.StringValue{"jp"}},
					{Category: query.CAT_TAGS, Operator: query.OP_EQ, Value: query.StringValue{"foobar"}},
					{Category: query.CAT_TITLE, Operator: query.OP_AP, Value: query.StringValue{"a very interesting title"}},
				},
				Clauses: []*query.Clause{
					{Operator: query.COP_OR, Statements: []query.Statement{{Category: query.CAT_AUTHOR, Operator: query.OP_NE, Value: query.StringValue{"pj"}}}},
					{Operator: query.COP_OR, Statements: []query.Statement{{Category: query.CAT_TAGS, Operator: query.OP_EQ, Value: query.StringValue{"barfoo"}}}},
				},
			},
			query.Clause{
				Operator: query.COP_OR,
				Statements: []query.Statement{
					{Category: query.CAT_TITLE, Operator: query.OP_AP, Value: query.StringValue{"a very interesting title"}},
					{Category: query.CAT_TAGS, Operator: query.OP_EQ, Value: query.StringValue{"foobar"}},
					{Category: query.CAT_AUTHOR, Operator: query.OP_AP, Value: query.StringValue{"jp"}},
					{Category: query.CAT_AUTHOR, Operator: query.OP_NE, Value: query.StringValue{"pj"}},
					{Category: query.CAT_TAGS, Operator: query.OP_EQ, Value: query.StringValue{"barfoo"}},
				},
			},
		},
		{
			"flatten 2 layers",
			&query.Clause{
				Operator: query.COP_AND,
				Statements: []query.Statement{
					{Category: query.CAT_TITLE, Operator: query.OP_AP, Value: query.StringValue{"a very interesting title"}},
					{Category: query.CAT_TAGS, Operator: query.OP_EQ, Value: query.StringValue{"foobar"}},
				},
				Clauses: []*query.Clause{
					{
						Operator: query.COP_AND,
						Statements: []query.Statement{
							{Category: query.CAT_AUTHOR, Operator: query.OP_AP, Value: query.StringValue{"jp"}},
							{Category: query.CAT_AUTHOR, Operator: query.OP_NE, Value: query.StringValue{"pj"}},
						},
						Clauses: []*query.Clause{
							{
								Operator: query.COP_AND,
								Statements: []query.Statement{
									{Category: query.CAT_TAGS, Operator: query.OP_EQ, Value: query.StringValue{"barfoo"}},
								},
							},
						},
					},
				},
			},
			query.Clause{
				Operator: query.COP_AND,
				Statements: []query.Statement{
					{Category: query.CAT_TITLE, Operator: query.OP_AP, Value: query.StringValue{"a very interesting title"}},
					{Category: query.CAT_TAGS, Operator: query.OP_EQ, Value: query.StringValue{"foobar"}},
					{Category: query.CAT_AUTHOR, Operator: query.OP_AP, Value: query.StringValue{"jp"}},
					{Category: query.CAT_AUTHOR, Operator: query.OP_NE, Value: query.StringValue{"pj"}},
					{Category: query.CAT_TAGS, Operator: query.OP_EQ, Value: query.StringValue{"barfoo"}},
				},
			},
		},
		{
			"flatten 1 child keep 1 child",
			&query.Clause{
				Operator: query.COP_AND,
				Statements: []query.Statement{
					{Category: query.CAT_TAGS, Operator: query.OP_EQ, Value: query.StringValue{"foobar"}},
					{Category: query.CAT_TITLE, Operator: query.OP_AP, Value: query.StringValue{"a very interesting title"}},
				},
				Clauses: []*query.Clause{
					{
						Operator: query.COP_OR,
						Statements: []query.Statement{
							{Category: query.CAT_AUTHOR, Operator: query.OP_AP, Value: query.StringValue{"jp"}},
							{Category: query.CAT_AUTHOR, Operator: query.OP_NE, Value: query.StringValue{"pj"}},
						},
					},
					{
						Operator: query.COP_AND,
						Statements: []query.Statement{
							{Category: query.CAT_TAGS, Operator: query.OP_EQ, Value: query.StringValue{"barfoo"}},
						},
					},
				},
			},
			query.Clause{
				Operator: query.COP_AND,
				Statements: []query.Statement{
					{Category: query.CAT_TAGS, Operator: query.OP_EQ, Value: query.StringValue{"foobar"}},
					{Category: query.CAT_TITLE, Operator: query.OP_AP, Value: query.StringValue{"a very interesting title"}},
					{Category: query.CAT_TAGS, Operator: query.OP_EQ, Value: query.StringValue{"barfoo"}},
				},
				Clauses: []*query.Clause{
					{
						Operator: query.COP_OR,
						Statements: []query.Statement{
							{Category: query.CAT_AUTHOR, Operator: query.OP_AP, Value: query.StringValue{"jp"}},
							{Category: query.CAT_AUTHOR, Operator: query.OP_NE, Value: query.StringValue{"pj"}},
						},
					},
				},
			},
		},
	}
	for _, tt := range tests {
		workers := uint(runtime.NumCPU())
		t.Run(tt.name, func(t *testing.T) {
			o := query.NewOptimizer(tt.root, workers)
			o.Flatten()

			slices.SortFunc(tt.root.Statements, query.StatementCmp)
			slices.SortFunc(tt.expected.Statements, query.StatementCmp)

			stmtsEq := slices.EqualFunc(tt.root.Statements, tt.expected.Statements,
				func(a, b query.Statement) bool {
					return a.Category == b.Category && a.Operator == b.Operator && a.Negated == b.Negated && a.Value.Compare(b.Value) == 0
				},
			)

			if !stmtsEq {
				t.Error("Statments not equal")
				if gL, wL := len(tt.root.Statements), len(tt.expected.Statements); gL != wL {
					t.Logf("Different number of statements: got %d want %d\n", gL, wL)
				}
			}

			gotL, wantL := len(tt.root.Clauses), len(tt.expected.Clauses)

			if gotL != wantL {
				t.Errorf("Incorrect number of children clauses: got %d want %d\n", gotL, wantL)
			}
		})
	}
}

func TestOptimizer_Compact(t *testing.T) {
	tests := []struct {
		name string
		c    *query.Clause
		want query.Clause
	}{
		{
			"already compact",
			&query.Clause{
				Statements: []query.Statement{
					{Category: query.CAT_AUTHOR, Operator: query.OP_EQ, Value: query.StringValue{"jp"}},
				},
			},
			query.Clause{
				Statements: []query.Statement{
					{Category: query.CAT_AUTHOR, Operator: query.OP_EQ, Value: query.StringValue{"jp"}},
				},
			},
		},
		{
			"can compact",
			&query.Clause{
				Statements: []query.Statement{
					{Category: query.CAT_AUTHOR, Operator: query.OP_EQ, Value: query.StringValue{"jp"}},
					{Negated: true, Category: query.CAT_AUTHOR, Operator: query.OP_NE, Value: query.StringValue{"jp"}},
				},
			},
			query.Clause{
				Statements: []query.Statement{
					{Category: query.CAT_AUTHOR, Operator: query.OP_EQ, Value: query.StringValue{"jp"}},
				},
			},
		},
		{
			"nested compact",
			&query.Clause{
				Statements: []query.Statement{
					{Category: query.CAT_AUTHOR, Operator: query.OP_EQ, Value: query.StringValue{"jp"}},
					{Negated: true, Category: query.CAT_AUTHOR, Operator: query.OP_NE, Value: query.StringValue{"jp"}},
				},
				Clauses: []*query.Clause{
					{
						Statements: []query.Statement{
							{Category: query.CAT_TITLE, Operator: query.OP_NE, Value: query.StringValue{"atlas"}},
							{Negated: true, Category: query.CAT_TITLE, Operator: query.OP_EQ, Value: query.StringValue{"atlas"}},
						},
					},
				},
			},
			query.Clause{
				Statements: []query.Statement{
					{Category: query.CAT_AUTHOR, Operator: query.OP_EQ, Value: query.StringValue{"jp"}},
				},
				Clauses: []*query.Clause{
					{
						Statements: []query.Statement{
							{Category: query.CAT_TITLE, Operator: query.OP_NE, Value: query.StringValue{"atlas"}},
						},
					},
				},
			},
		},
	}
	for _, tt := range tests {
		workers := uint(runtime.NumCPU())
		t.Run(tt.name, func(t *testing.T) {
			o := query.NewOptimizer(tt.c, workers)
			o.Compact()
			got := slices.Collect(tt.c.DFS())
			want := slices.Collect(tt.want.DFS())

			gotL, wantL := len(got), len(want)
			if gotL != wantL {
				// only happens if written test case incorrectly
				t.Errorf("Different number of clauses: got %d want %d", gotL, wantL)
			}

			for i := range min(gotL, wantL) {
				gotClause, wantClause := got[i], want[i]

				if gOp, wOp := gotClause.Operator, wantClause.Operator; gOp != wOp {
					t.Errorf("Different operator for clause %d: want %v, got %v", i, gOp, wOp)
				}

				if !slices.Equal(gotClause.Statements, wantClause.Statements) {
					t.Errorf("Different statements for clause %d", i)
					t.Log("Got", gotClause.Statements)
					t.Log("Want", wantClause.Statements)
				}
			}
		})
	}
}
