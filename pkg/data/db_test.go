package data_test

import (
	"slices"
	"testing"

	"github.com/jpappel/atlas/pkg/data"
)

func TestBatchQuery(t *testing.T) {
	tests := []struct {
		name      string
		query     string
		start     string
		val       string
		delim     string
		stop      string
		n         int
		args      []int
		wantQuery string
		wantArgs  []any
	}{
		{
			"1 val group",
			"INSERT INTO Foo VALUES",
			"(",
			"?",
			",",
			")",
			5,
			[]int{1, 2, 3, 4, 5},
			"INSERT INTO Foo VALUES (?,?,?,?,?)",
			[]any{1, 2, 3, 4, 5},
		},
		{
			"multiple val groups",
			"INSERT INTO Bar VALUES",
			"",
			"(?,?)",
			",",
			"",
			2,
			[]int{1, 2, 3, 4},
			"INSERT INTO Bar VALUES (?,?),(?,?)",
			[]any{1, 2, 3, 4},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotQuery, gotArgs := data.BatchQuery(tt.query, tt.start, tt.val, tt.delim, tt.stop, tt.n, tt.args)
			if gotQuery != tt.wantQuery {
				t.Error("Got different query than wanted")
				t.Log("Wanted:\n" + tt.wantQuery)
				t.Log("Got:\n" + gotQuery)
			}

			if !slices.Equal(tt.wantArgs, gotArgs) {
				t.Error("Got different args than wanted")
				t.Logf("Wanted:\t%v", tt.wantArgs)
				t.Logf("Got:\t%v", gotArgs)
			}
		})
	}
}
