package data_test

import (
	"database/sql"
	"errors"
	"testing"
	"time"

	"github.com/jpappel/atlas/pkg/data"
	"github.com/jpappel/atlas/pkg/index"
)

func singleDoc(t *testing.T) *sql.DB {
	t.Helper()
	db := data.NewMemDB()

	if _, err := db.Exec(`
	INSERT INTO Documents (path, title, date, fileTime)
	VALUES ("/file", "A file", 1, 2)
	`); err != nil {
		t.Fatal("err inserting doc:", err)
	}

	if _, err := db.Exec(`
	INSERT INTO Authors (name)
	VALUES ("jp")
	`); err != nil {
		t.Fatal("err inserting author:", err)
	}

	if _, err := db.Exec(`
	INSERT INTO Aliases (authorId, alias)
	VALUES (1,"pj"), (1,"JP")
	`); err != nil {
		t.Fatal("err inserting aliases:", err)
	}

	if _, err := db.Exec(`
	INSERT INTO Tags (name)
	VALUES ("foo"), ("bar"), ("baz"), ("oof")
	`); err != nil {
		t.Fatal("err inserting tags:", err)
	}

	if _, err := db.Exec(`
	INSERT INTO DocumentAuthors (docId, authorId)
	VALUES (1, 1)
	`); err != nil {
		t.Fatal("err inserting docAuthors:", err)
	}

	if _, err := db.Exec(`
	INSERT INTO DocumentTags (docId, tagId)
	VALUES (1,1), (1,2), (1,3), (1,4)
	`); err != nil {
		t.Fatal("err inserting docTags:", err)
	}

	if _, err := db.Exec(`
	INSERT INTO Links (docId, link)
	VALUES (1, 'link1'), (1, 'link2')
	`); err != nil {
		t.Fatal("err inserting links:", err)
	}

	return db
}

func multiDoc(t *testing.T) *sql.DB {
	t.Helper()
	db := data.NewMemDB()

	if _, err := db.Exec(`
	INSERT INTO Documents (path, title, date, fileTime)
	VALUES ("/notes/anote.md", "A note", 1, 2), ("README.md", "read this file!", 3, 4)
	`); err != nil {
		t.Fatal("err inserting doc:", err)
	}

	if _, err := db.Exec(`
	INSERT INTO Authors (name)
	VALUES ("jp"), ("anonymous")
	`); err != nil {
		t.Fatal("err inserting author:", err)
	}

	if _, err := db.Exec(`
	INSERT INTO Aliases (authorId, alias)
	VALUES (1,"pj"), (1,"JP")
	`); err != nil {
		t.Fatal("err inserting aliases:", err)
	}

	if _, err := db.Exec(`
	INSERT INTO Tags (name)
	VALUES ("foo"), ("bar"), ("baz"), ("oof")
	`); err != nil {
		t.Fatal("err inserting tags:", err)
	}

	if _, err := db.Exec(`
	INSERT INTO DocumentAuthors (docId, authorId)
	VALUES (1, 1), (2, 2), (2, 1)
	`); err != nil {
		t.Fatal("err inserting docAuthors:", err)
	}

	if _, err := db.Exec(`
	INSERT INTO DocumentTags (docId, tagId)
	VALUES (1,1), (2,2), (1,3), (2,4)
	`); err != nil {
		t.Fatal("err inserting docTags:", err)
	}

	if _, err := db.Exec(`
	INSERT INTO Links (docId, link)
	VALUES (1, '/home'), (2, 'rsync://rsync.kernel.org/pub/')
	`); err != nil {
		t.Fatal("err inserting links:", err)
	}

	return db
}

func TestFill_Get(t *testing.T) {
	tests := []struct {
		name    string
		newFill func(t *testing.T) data.Fill
		want    index.Document
		wantErr error
	}{
		{
			"single doc",
			func(t *testing.T) data.Fill {
				t.Helper()
				return data.Fill{Path: "/file", Db: singleDoc(t)}
			},
			index.Document{
				Path:     "/file",
				Title:    "A file",
				Date:     time.Unix(1, 0),
				FileTime: time.Unix(2, 0),
				Authors:  []string{"jp"},
				Tags:     []string{"foo", "bar", "oof", "baz"},
				Links:    []string{"link1", "link2"},
			},
			nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := tt.newFill(t)
			got, gotErr := f.Get(t.Context())

			if !errors.Is(gotErr, tt.wantErr) {
				t.Fatalf("Recieved unexpected error: got %v want %v", gotErr, tt.wantErr)
			} else if gotErr != nil {
				return
			}

			if !got.Equal(tt.want) {
				t.Errorf("Get() = %+v\nWant %+v", got, tt.want)
			}
		})
	}
}

func TestFillMany_Get(t *testing.T) {
	tests := []struct {
		name        string
		newFillMany func(t *testing.T) data.FillMany
		want        map[string]*index.Document
		wantErr     error
	}{
		{
			"multi doc",
			func(t *testing.T) data.FillMany {
				t.Helper()
				return data.FillMany{Db: multiDoc(t)}
			},
			map[string]*index.Document{
				"/notes/anote.md": {
					Path:     "/notes/anote.md",
					Title:    "A note",
					Date:     time.Unix(1, 0),
					FileTime: time.Unix(2, 0),
					Authors:  []string{"jp"},
					Tags:     []string{"foo", "baz"},
					Links:    []string{"/home"},
				},
				"README.md": {
					Path:     "README.md",
					Title:    "read this file!",
					Date:     time.Unix(3, 0),
					FileTime: time.Unix(4, 0),
					Authors:  []string{"anonymous", "jp"},
					Tags:     []string{"bar", "oof"},
					Links:    []string{"rsync://rsync.kernel.org/pub/"},
				},
			},
			nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := t.Context()
			f := tt.newFillMany(t)

			got, gotErr := f.Get(ctx)
			if !errors.Is(gotErr, tt.wantErr) {
				t.Fatalf("Recieved unexpected error: got %v want %v", gotErr, tt.wantErr)
			} else if gotErr != nil {
				return
			}

			if len(tt.want) != len(got) {
				t.Errorf("Recieved incorrect amount of documents: got %d want %d", len(got), len(tt.want))
			}

			for path, wantDoc := range tt.want {
				gotDoc, ok := got[path]
				if !ok {
					t.Errorf("Can't find %s in recieved docs", path)
					continue
				}

				if !gotDoc.Equal(*wantDoc) {
					t.Errorf("%s not equal %+v\nWant %+v", path, gotDoc, wantDoc)
				}
			}
		})
	}
}
