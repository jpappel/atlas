package query_test

import (
	"runtime"
	"slices"
	"testing"
	"time"

	"github.com/jpappel/atlas/pkg/query"
)

var WORKERS = uint(runtime.NumCPU())

func clauseEqTest(t *testing.T, gotClause *query.Clause, wantClause *query.Clause) {
	t.Helper()
	o1 := query.NewOptimizer(gotClause, WORKERS)
	o1.SortStatements()
	o2 := query.NewOptimizer(wantClause, WORKERS)
	o2.SortStatements()

	got := slices.Collect(gotClause.DFS())
	want := slices.Collect(wantClause.DFS())
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
}

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
		t.Run(tt.name, func(t *testing.T) {
			o := query.NewOptimizer(tt.root, WORKERS)
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
		t.Run(tt.name, func(t *testing.T) {
			o := query.NewOptimizer(tt.c, WORKERS)
			o.Compact()

			clauseEqTest(t, tt.c, &tt.want)
		})
	}
}

func TestOptimizer_Tidy(t *testing.T) {
	tests := []struct {
		name string
		c    *query.Clause
		want query.Clause
	}{
		{
			"already tidy",
			&query.Clause{
				Operator: query.COP_AND,
				Statements: []query.Statement{
					{Category: query.CAT_TITLE, Operator: query.OP_AP, Value: query.StringValue{"manufacturing"}},
				},
				Clauses: []*query.Clause{
					{Operator: query.COP_OR, Statements: []query.Statement{
						{false, query.CAT_AUTHOR, query.OP_EQ, query.StringValue{"Chomsky, Noam"}},
						{false, query.CAT_AUTHOR, query.OP_EQ, query.StringValue{"Noam Chomsky"}},
					}},
				},
			},
			query.Clause{
				Operator: query.COP_AND,
				Statements: []query.Statement{
					{Category: query.CAT_TITLE, Operator: query.OP_AP, Value: query.StringValue{"manufacturing"}},
				},
				Clauses: []*query.Clause{
					{Operator: query.COP_OR, Statements: []query.Statement{
						{false, query.CAT_AUTHOR, query.OP_EQ, query.StringValue{"Chomsky, Noam"}},
						{false, query.CAT_AUTHOR, query.OP_EQ, query.StringValue{"Noam Chomsky"}},
					}},
				},
			},
		},
		{
			"top level tidy",
			&query.Clause{
				Operator: query.COP_AND,
				Statements: []query.Statement{
					{Category: query.CAT_TITLE, Operator: query.OP_AP, Value: query.StringValue{"manufacturing"}},
				},
				Clauses: []*query.Clause{
					{Operator: query.COP_OR, Statements: []query.Statement{
						{false, query.CAT_AUTHOR, query.OP_EQ, query.StringValue{"Chomsky, Noam"}},
						{false, query.CAT_AUTHOR, query.OP_EQ, query.StringValue{"Noam Chomsky"}},
						{},
						{Category: 2 << 16},
					}},
				},
			},
			query.Clause{
				Operator: query.COP_AND,
				Statements: []query.Statement{
					{Category: query.CAT_TITLE, Operator: query.OP_AP, Value: query.StringValue{"manufacturing"}},
				},
				Clauses: []*query.Clause{
					{Operator: query.COP_OR, Statements: []query.Statement{
						{false, query.CAT_AUTHOR, query.OP_EQ, query.StringValue{"Chomsky, Noam"}},
						{false, query.CAT_AUTHOR, query.OP_EQ, query.StringValue{"Noam Chomsky"}},
					}},
				},
			},
		},
		{
			"nested tidy",
			&query.Clause{
				Operator: query.COP_AND,
				Statements: []query.Statement{
					{Category: query.CAT_TITLE, Operator: query.OP_AP, Value: query.StringValue{"industry"}},
					{true, query.CAT_AUTHOR, query.OP_EQ, query.StringValue{"Alan Dersowitz"}},
				},
				Clauses: []*query.Clause{
					{Operator: query.COP_OR, Statements: []query.Statement{
						{false, query.CAT_AUTHOR, query.OP_EQ, query.StringValue{"Finkelstein, Norman"}},
						{false, query.CAT_AUTHOR, query.OP_EQ, query.StringValue{"Norman Finkelstein"}},
						{false, query.CAT_AUTHOR, query.OP_EQ, query.StringValue{"Norm Finkelstein"}},
						{},
						{Category: CAT_META + 1},
					}},
				},
			},
			query.Clause{
				Operator: query.COP_AND,
				Statements: []query.Statement{
					{Category: query.CAT_TITLE, Operator: query.OP_AP, Value: query.StringValue{"industry"}},
					{true, query.CAT_AUTHOR, query.OP_EQ, query.StringValue{"Alan Dersowitz"}},
				},
				Clauses: []*query.Clause{
					{Operator: query.COP_OR, Statements: []query.Statement{
						{false, query.CAT_AUTHOR, query.OP_EQ, query.StringValue{"Finkelstein, Norman"}},
						{false, query.CAT_AUTHOR, query.OP_EQ, query.StringValue{"Norman Finkelstein"}},
						{false, query.CAT_AUTHOR, query.OP_EQ, query.StringValue{"Norm Finkelstein"}},
					}},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			o := query.NewOptimizer(tt.c, WORKERS)
			o.Tidy()
			clauseEqTest(t, tt.c, &tt.want)
		})
	}
}

func TestOptimizer_Contradictions(t *testing.T) {
	tests := []struct {
		name string
		c    *query.Clause
		want query.Clause
	}{
		{
			"two equals",
			&query.Clause{
				Operator: query.COP_AND,
				Statements: []query.Statement{
					{Category: CAT_TITLE, Operator: OP_EQ, Value: query.StringValue{"carnival"}},
					{Category: CAT_TITLE, Operator: OP_EQ, Value: query.StringValue{"carnivale"}},
				},
			},
			query.Clause{
				Operator: query.COP_AND,
			},
		},
		{
			"equal and not equal",
			&query.Clause{
				Operator: query.COP_AND,
				Statements: []query.Statement{
					{Category: CAT_TITLE, Operator: OP_EQ, Value: query.StringValue{"apple"}},
					{Negated: true, Category: CAT_TITLE, Operator: OP_EQ, Value: query.StringValue{"apple"}},
					{Category: CAT_TITLE, Operator: OP_NE, Value: query.StringValue{"apple"}},
				},
			},
			query.Clause{
				Operator: query.COP_AND,
			},
		},
		{
			"set contradiction",
			&query.Clause{
				Operator: query.COP_AND,
				Statements: []query.Statement{
					{Category: CAT_TAGS, Operator: OP_EQ, Value: query.StringValue{"topology"}},
					{Category: CAT_TAGS, Operator: OP_NE, Value: query.StringValue{"topology"}},
				},
				Clauses: []*query.Clause{{
					Operator: query.COP_OR,
					Statements: []query.Statement{
						{Category: CAT_TAGS, Operator: OP_EQ, Value: query.StringValue{"algebra"}},
						{Category: CAT_TAGS, Operator: OP_EQ, Value: query.StringValue{"differential"}},
						{Category: CAT_TAGS, Operator: OP_EQ, Value: query.StringValue{"geometric"}},
					},
				}},
			},
			query.Clause{
				Operator:   query.COP_AND,
				Statements: []query.Statement{},
				Clauses: []*query.Clause{{
					Operator: query.COP_OR,
					Statements: []query.Statement{
						{Category: CAT_TAGS, Operator: OP_EQ, Value: query.StringValue{"algebra"}},
						{Category: CAT_TAGS, Operator: OP_EQ, Value: query.StringValue{"differential"}},
						{Category: CAT_TAGS, Operator: OP_EQ, Value: query.StringValue{"geometric"}},
					},
				}},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			o := query.NewOptimizer(tt.c, WORKERS)
			o.Contradictions()
			o.Tidy()

			clauseEqTest(t, tt.c, &tt.want)
		})
	}
}

func TestOptimizer_StrictEquality(t *testing.T) {
	tests := []struct {
		name string
		c    *query.Clause
		want query.Clause
	}{
		{
			"non-range, non-set",
			&query.Clause{
				Operator: query.COP_AND,
				Statements: []query.Statement{
					{Category: CAT_TITLE, Operator: OP_EQ, Value: query.StringValue{"notes"}},
					{Category: CAT_TITLE, Operator: OP_AP, Value: query.StringValue{"monday standup"}},
				},
			},
			query.Clause{
				Operator: query.COP_AND,
				Statements: []query.Statement{
					{Category: CAT_TITLE, Operator: OP_EQ, Value: query.StringValue{"notes"}},
				},
			},
		},
		{
			"set",
			&query.Clause{
				Operator: query.COP_AND,
				Statements: []query.Statement{
					{Category: CAT_AUTHOR, Operator: OP_EQ, Value: query.StringValue{"Alonzo Church"}},
					{Category: CAT_AUTHOR, Operator: OP_EQ, Value: query.StringValue{"Alan Turing"}},
					{Category: CAT_AUTHOR, Operator: OP_AP, Value: query.StringValue{"turing"}},
				},
			},
			query.Clause{
				Operator: query.COP_AND,
				Statements: []query.Statement{
					{Category: CAT_AUTHOR, Operator: OP_EQ, Value: query.StringValue{"Alonzo Church"}},
					{Category: CAT_AUTHOR, Operator: OP_EQ, Value: query.StringValue{"Alan Turing"}},
				},
			},
		},
		{
			"set, no strict eq",
			&query.Clause{
				Operator: query.COP_AND,
				Statements: []query.Statement{
					{Category: CAT_AUTHOR, Operator: OP_EQ, Value: query.StringValue{"Alonzo Church"}},
					{Category: CAT_AUTHOR, Operator: OP_EQ, Value: query.StringValue{"Alan Turing"}},
					{Category: CAT_AUTHOR, Operator: OP_AP, Value: query.StringValue{"djikstra"}},
				},
			},
			query.Clause{
				Operator: query.COP_AND,
				Statements: []query.Statement{
					{Category: CAT_AUTHOR, Operator: OP_EQ, Value: query.StringValue{"Alonzo Church"}},
					{Category: CAT_AUTHOR, Operator: OP_EQ, Value: query.StringValue{"Alan Turing"}},
					{Category: CAT_AUTHOR, Operator: OP_AP, Value: query.StringValue{"djikstra"}},
				},
			},
		},
		{
			"dates",
			&query.Clause{
				Operator: query.COP_AND,
				Statements: []query.Statement{
					{Category: CAT_DATE, Operator: OP_EQ, Value: query.DatetimeValue{time.Date(1886, time.May, 1, 0, 0, 0, 0, time.UTC)}},
					{Category: CAT_DATE, Operator: OP_GE, Value: query.DatetimeValue{time.Date(1880, time.January, 1, 0, 0, 0, 0, time.UTC)}},
				},
			},
			query.Clause{
				Operator: query.COP_AND,
				Statements: []query.Statement{
					{Category: CAT_DATE, Operator: OP_EQ, Value: query.DatetimeValue{time.Date(1886, time.May, 1, 0, 0, 0, 0, time.UTC)}},
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			o := query.NewOptimizer(tt.c, WORKERS)
			o.StrictEquality()
			o.Tidy()

			clauseEqTest(t, tt.c, &tt.want)
		})
	}
}

func TestOptimizer_Tighten(t *testing.T) {
	tests := []struct {
		name string
		c    *query.Clause
		want query.Clause
	}{
		{
			"dates or",
			&query.Clause{
				Operator: query.COP_OR,
				Statements: []query.Statement{
					{Category: query.CAT_DATE, Operator: query.OP_GT, Value: query.DatetimeValue{time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)}},
					{Category: query.CAT_DATE, Operator: query.OP_GT, Value: query.DatetimeValue{time.Date(2025, 2, 2, 0, 0, 0, 0, time.UTC)}},
				},
			},
			query.Clause{
				Operator: query.COP_OR,
				Statements: []query.Statement{
					{Category: query.CAT_DATE, Operator: query.OP_GT, Value: query.DatetimeValue{time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)}},
				},
			},
		},
		{
			"dates and",
			&query.Clause{
				Operator: query.COP_AND,
				Statements: []query.Statement{
					{Category: query.CAT_DATE, Operator: query.OP_GT, Value: query.DatetimeValue{time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)}},
					{Category: query.CAT_DATE, Operator: query.OP_GT, Value: query.DatetimeValue{time.Date(2025, 2, 2, 0, 0, 0, 0, time.UTC)}},
				},
			},
			query.Clause{
				Operator: query.COP_AND,
				Statements: []query.Statement{
					{Category: query.CAT_DATE, Operator: query.OP_GT, Value: query.DatetimeValue{time.Date(2025, 2, 2, 0, 0, 0, 0, time.UTC)}},
				},
			},
		},
		{
			"nonordered or",
			&query.Clause{
				Operator: query.COP_OR,
				Statements: []query.Statement{
					{Category: CAT_TITLE, Operator: OP_AP, Value: query.StringValue{"Das Kapital I"}},
					{Category: CAT_TITLE, Operator: OP_AP, Value: query.StringValue{"Das Kapital"}},
				},
			},
			query.Clause{
				Operator: query.COP_OR,
				Statements: []query.Statement{
					{Category: CAT_TITLE, Operator: OP_AP, Value: query.StringValue{"Das Kapital"}},
				},
			},
		},
		{
			"nonordered and",
			&query.Clause{
				Operator: query.COP_AND,
				Statements: []query.Statement{
					{Category: CAT_TITLE, Operator: OP_AP, Value: query.StringValue{"Das Kapital I"}},
					{Category: CAT_TITLE, Operator: OP_AP, Value: query.StringValue{"Das Kapital"}},
				},
			},
			query.Clause{
				Operator: query.COP_AND,
				Statements: []query.Statement{
					{Category: CAT_TITLE, Operator: OP_AP, Value: query.StringValue{"Das Kapital I"}},
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			o := query.NewOptimizer(tt.c, WORKERS)
			o.Tighten()
			o.Tidy()

			clauseEqTest(t, tt.c, &tt.want)
		})
	}
}
