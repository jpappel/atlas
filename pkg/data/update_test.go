package data_test

import (
	"context"
	"database/sql"
	"errors"
	"maps"
	"slices"
	"testing"
	"time"

	"github.com/jpappel/atlas/pkg/data"
	"github.com/jpappel/atlas/pkg/index"
)

func TestUpdate_Update(t *testing.T) {
	tests := []struct {
		name    string
		newDb   func(t *testing.T) *sql.DB
		doc     index.Document
		wantErr error
	}{
		{
			"update on empty",
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
				Headings: "#A Heading\n",
				Links:    []string{"link_1", "link_2", "link_3"},
			},
			nil,
		},
		{
			"update on existing",
			func(t *testing.T) *sql.DB {
				t.Helper()
				db := data.NewMemDB("test")
				p := data.NewPut(db, index.Document{
					Path:     "/file",
					Title:    "A file",
					Date:     time.Unix(1, 0),
					FileTime: time.Unix(2, 0),
					Authors:  []string{"jp"},
					Tags:     []string{"foo", "bar", "oof", "baz"},
					Headings: "#Old Heading\n",
					Links:    []string{"link_1", "link_2", "link_3"},
				})

				if err := p.Insert(t.Context()); err != nil {
					panic(err)
				}

				return db
			},
			index.Document{
				Path:     "/file",
				Title:    "A file with a new title",
				Date:     time.Unix(1, 0),
				FileTime: time.Unix(3, 0),
				Authors:  []string{"jp", "pj"},
				Tags:     []string{"foo", "bar", "oof"},
				Headings: "#New Heading\n",
				Links:    []string{"link_4"},
			},
			nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db := tt.newDb(t)
			defer db.Close()

			u := data.NewUpdate(context.Background(), db, tt.doc)
			gotErr := u.Update(t.Context())
			if !errors.Is(gotErr, tt.wantErr) {
				t.Fatalf("recieved unexpected error: got %v want %v", gotErr, tt.wantErr)
			} else if gotErr != nil {
				return
			}

			f := data.Fill{Path: tt.doc.Path, Db: db}
			gotDoc, err := f.Get(t.Context())
			if err != nil {
				t.Fatal("Error while retrieving document for comparison:", err)
			}

			if !gotDoc.Equal(tt.doc) {
				t.Errorf("Retrieved doc is not stored doc!\nrecv: %+v\nsent: %+v", gotDoc, tt.doc)
			}
		})
	}
}

func TestUpdateMany_Update(t *testing.T) {
	tests := []struct {
		name    string
		newDb   func(t *testing.T) *sql.DB
		docs    map[string]*index.Document
		wantErr error
	}{
		{
			"additions",
			func(t *testing.T) *sql.DB {
				return data.NewMemDB("test")
			},
			map[string]*index.Document{
				"/afile": {
					Path:     "/afile",
					Title:    "A file",
					Date:     time.Unix(1, 0),
					FileTime: time.Unix(2, 0),
					Authors:  []string{"jp"},
					Tags:     []string{"foo", "bar", "oof", "baz"},
					Headings: "# Some Heading\n",
					Links:    []string{"link_1", "link_2", "link_3"},
				},
				"/bfile": {
					Path:     "/bfile",
					Title:    "B file",
					Date:     time.Unix(3, 0),
					FileTime: time.Unix(4, 0),
					Authors:  []string{"pj"},
					Tags:     []string{"foo", "gar"},
					Links:    []string{"link_4"},
				},
			},
			nil,
		},
		{
			"delete",
			func(t *testing.T) *sql.DB {
				db := data.NewMemDB("test")

				docs := map[string]*index.Document{
					"/afile": {
						Path:     "/afile",
						Title:    "A file",
						Date:     time.Unix(1, 0),
						FileTime: time.Unix(2, 0),
						Authors:  []string{"jp"},
						Tags:     []string{"foo", "bar", "oof", "baz"},
						Links:    []string{"link_1", "link_2", "link_3"},
					},
					"/bfile": {
						Path:     "/bfile",
						Title:    "B file",
						Date:     time.Unix(3, 0),
						FileTime: time.Unix(4, 0),
						Authors:  []string{"pj"},
						Tags:     []string{"foo", "gar"},
						Links:    []string{"link_4"},
					},
				}
				p, err := data.NewPutMany(t.Context(), db, docs)
				if err != nil {
					panic(err)
				}
				if err := p.Insert(); err != nil {
					panic(err)
				}

				return db
			},
			map[string]*index.Document{
				"/afile": {
					Path:     "/afile",
					Title:    "A file",
					Date:     time.Unix(1, 0),
					FileTime: time.Unix(2, 0),
					Authors:  []string{"jp"},
					Tags:     []string{"foo", "bar", "oof", "baz"},
					Links:    []string{"link_1", "link_2", "link_3"},
				},
			},
			nil,
		},
		{
			"update",
			func(t *testing.T) *sql.DB {
				db := data.NewMemDB("test")

				docs := map[string]*index.Document{
					"/afile": {
						Path:     "/afile",
						Title:    "A file",
						Date:     time.Unix(1, 0),
						FileTime: time.Unix(2, 0),
						Authors:  []string{"jp"},
						Tags:     []string{"foo", "bar", "oof", "baz"},
						Headings: "# A Original\n",
						Links:    []string{"link_1", "link_2", "link_3"},
					},
					"/bfile": {
						Path:     "/bfile",
						Title:    "B file",
						Date:     time.Unix(3, 0),
						FileTime: time.Unix(4, 0),
						Authors:  []string{"pj"},
						Tags:     []string{"foo", "gar"},
						Headings: "# B Original\n",
						Links:    []string{"link_4"},
					},
				}
				p, err := data.NewPutMany(t.Context(), db, docs)
				if err != nil {
					panic(err)
				}
				if err := p.Insert(); err != nil {
					panic(err)
				}

				return db
			},
			map[string]*index.Document{
				"/afile": {
					Path:     "/afile",
					Title:    "A file",
					Date:     time.Unix(1, 0),
					FileTime: time.Unix(10, 0),
					Authors:  []string{"jp"},
					Tags:     []string{"foo", "bar", "bing", "baz"},
					Headings: "# A New\n",
					Links:    []string{"link_1", "link_3"},
				},
				"/bfile": {
					Path:     "/bfile",
					Title:    "B file",
					Date:     time.Unix(3, 0),
					FileTime: time.Unix(5, 0),
					Authors:  []string{},
					Tags:     []string{},
					Links:    []string{},
				},
			},
			nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db := tt.newDb(t)
			defer db.Close()

			u := data.UpdateMany{Db: db, PathDocs: tt.docs}
			gotErr := u.Update(t.Context())
			if !errors.Is(gotErr, tt.wantErr) {
				t.Fatalf("recieved unexpected error: got %v want %v", gotErr, tt.wantErr)
			} else if gotErr != nil {
				return
			}

			f := data.FillMany{Db: db}
			docs, err := f.Get(t.Context())
			if err != nil {
				t.Fatal("Error while retrieving documents for comparison:", err)
			}

			if !maps.EqualFunc(docs, tt.docs, func(a, b *index.Document) bool {
				return a.Equal(*b)
			}) {
				t.Error("Got different docs than expected")
				if len(docs) != len(tt.docs) {
					t.Logf("Wanted %d docs, got %d", len(tt.docs), len(docs))
				}

				for path, wantDoc := range tt.docs {
					gotDoc, ok := docs[path]
					if !ok {
						t.Logf("Wanted doc at %s but did not recieve it", path)
						continue
					} else if wantDoc.Equal(*gotDoc) {
						continue
					}

					t.Log("Doc: ", path)
					if wantDoc.Title != gotDoc.Title {
						t.Log("want Title:", wantDoc.Title)
						t.Log("Got Title:", gotDoc.Title)
					}
					if !wantDoc.Date.Equal(gotDoc.Date) {
						t.Log("want Date:", wantDoc.Date)
						t.Log("got Date:", gotDoc.Date)
					}
					if !wantDoc.FileTime.Equal(gotDoc.FileTime) {
						t.Log("want filetime:", wantDoc.FileTime)
						t.Log("got filetime:", gotDoc.FileTime)
					}
					if !slices.Equal(wantDoc.Authors, gotDoc.Authors) {
						t.Log("want authors:", wantDoc.Authors)
						t.Log("got authors:", gotDoc.Authors)
					}
					if !slices.Equal(wantDoc.Tags, gotDoc.Tags) {
						t.Log("want tags:", wantDoc.Tags)
						t.Log("got tags:", gotDoc.Tags)
					}
					if !slices.Equal(wantDoc.Links, gotDoc.Links) {
						t.Log("want links:", wantDoc.Links)
						t.Log("got links:", gotDoc.Links)
					}
					if wantDoc.Headings != gotDoc.Headings {
						t.Log("want headings:", wantDoc.Headings)
						t.Log("got headings:", gotDoc.Headings)
					}
					if wantDoc.OtherMeta != gotDoc.OtherMeta {
						t.Log("want meta:", wantDoc.OtherMeta)
						t.Log("got meta:", gotDoc.OtherMeta)
					}
				}
			}
		})
	}
}
