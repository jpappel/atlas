package data_test

import (
	"context"
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
				return data.NewMemDB()
			},
			index.Document{
				Path:     "/file",
				Title:    "A file",
				Date:     time.Unix(1, 0),
				FileTime: time.Unix(2, 0),
				Authors:  []string{"jp"},
				Tags:     []string{"foo", "bar", "oof", "baz"},
			},
			nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			db := tt.newDb(t)
			defer db.Close()

			p, err := data.NewPut(ctx, db, tt.doc)
			if err != nil {
				t.Fatalf("could not construct receiver type: %v", err)
			}

			gotErr := p.Insert()
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
		name string // description of this test case
		// Named input parameters for receiver constructor.
		db        *sql.DB
		documents map[string]*index.Document
		wantErr   bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p, err := data.NewPutMany(tt.db, tt.documents)
			if err != nil {
				t.Fatalf("could not construct receiver type: %v", err)
			}
			gotErr := p.Insert(context.Background())
			if gotErr != nil {
				if !tt.wantErr {
					t.Errorf("Insert() failed: %v", gotErr)
				}
				return
			}
			if tt.wantErr {
				t.Fatal("Insert() succeeded unexpectedly")
			}
		})
	}
}

