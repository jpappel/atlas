package data_test

import (
	"database/sql"
	"errors"
	"testing"
	"time"

	"github.com/jpappel/atlas/pkg/data"
	"github.com/jpappel/atlas/pkg/index"
)

func TestPut_Insert(t *testing.T) {
	tests := []struct {
		name    string
		newDb   func(t *testing.T) *sql.DB
		doc     index.Document
		wantErr error
	}{
		{
			"insert on empty",
			func(t *testing.T) *sql.DB {
				t.Helper()
				return data.NewMemDB("test")
			},
			index.Document{
				Path:     "/file",
				Title:    "A file",
				Date:     time.Unix(1, 0),
				FileTime: time.Unix(2, 0),
				Authors:  []string{"jp"},
				Tags:     []string{"foo", "bar", "oof", "baz"},
				Links:    []string{"link_1", "link_2", "link_3"},
			},
			nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := t.Context()
			db := tt.newDb(t)
			defer db.Close()

			p := data.NewPut(db, tt.doc)
			gotErr := p.Insert(t.Context())
			if !errors.Is(gotErr, tt.wantErr) {
				t.Fatalf("Unexpected error on Insert():, want %v got %v", tt.wantErr, gotErr)
			} else if gotErr != nil {
				return
			}

			f := data.Fill{Path: tt.doc.Path, Db: db}
			gotDoc, err := f.Get(ctx)
			if err != nil {
				t.Fatal("Error while retrieving document for comparison:", err)
			}

			if !gotDoc.Equal(tt.doc) {
				t.Errorf("Retrieved doc is not stored doc!\nrecv: %+v\nsent: %+v", gotDoc, tt.doc)
			}
		})
	}
}

func TestPutMany_Insert(t *testing.T) {
	tests := []struct {
		name      string
		newDb     func(t *testing.T) *sql.DB
		documents map[string]*index.Document
		wantErr   error
	}{
		{
			name: "insert on empty",
			newDb: func(t *testing.T) *sql.DB {
				t.Helper()
				return data.NewMemDB("test")
			},
			documents: map[string]*index.Document{
				"/file": {
					Path:     "/file",
					Title:    "A file",
					Date:     time.Unix(1, 0),
					FileTime: time.Unix(2, 0),
					Authors:  []string{"jp"},
					Tags:     []string{"foo", "bar", "oof", "baz"},
					Links:    []string{"link_1", "link_2", "link_3"},
				},
				"/file2": {
					Path:     "/file2",
					Title:    "A different file",
					Date:     time.Unix(3, 0),
					FileTime: time.Unix(4, 0),
					Authors:  []string{"pj"},
					Tags:     []string{"apple", "pear", "peach"},
					Links:    []string{"a very useful link"},
				},
			},
			wantErr: nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db := tt.newDb(t)
			p, err := data.NewPutMany(t.Context(), db, tt.documents)
			if err != nil {
				t.Fatalf("could not construct receiver type: %v", err)
			}

			gotErr := p.Insert()
			if !errors.Is(gotErr, tt.wantErr) {
				t.Fatalf("Recieved unexpected error, got %v want %v", gotErr, tt.wantErr)
			} else if err != nil {
				return
			}

			f := data.FillMany{Db: db}
			gotDocs, err := f.Get(t.Context())
			if err != nil {
				t.Fatal("Error while retrieving documents for comparison:", err)
			}

			wantLen, gotLen := len(tt.documents), len(gotDocs)
			if wantLen != gotLen {
				t.Fatalf("Recieved differnt number of documents than expected: got %d, want %d", gotLen, wantLen)
			}

			for path, wantDoc := range tt.documents {
				gotDoc, ok := gotDocs[path]
				if !ok {
					t.Errorf("Wanted doc with path %s but did not recieve it", path)
				}

				if !wantDoc.Equal(*gotDoc) {
					t.Errorf("Difference betwen docs!\ngot: %+v\nwant: %+v", gotDoc, wantDoc)
				}
			}
		})
	}
}
