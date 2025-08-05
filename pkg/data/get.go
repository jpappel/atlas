package data

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/jpappel/atlas/pkg/index"
)

// Use to build a document from a database connection
type Fill struct {
	Path string
	Db   *sql.DB
	id   int
	doc  *index.Document
}

// Use to build documents and aliases from a database connection
type FillMany struct {
	docs map[string]*index.Document
	ids  map[string]int
	Db   *sql.DB
}

func (f Fill) Get(ctx context.Context) (*index.Document, error) {
	f.doc = &index.Document{Path: f.Path}
	if err := f.document(ctx); err != nil {
		return nil, err
	}
	if err := f.tags(ctx); err != nil {
		return nil, err
	}
	if err := f.authors(ctx); err != nil {
		return nil, err
	}
	if err := f.links(ctx); err != nil {
		return nil, err
	}

	return f.doc, nil
}

func (f *FillMany) Get(ctx context.Context) (map[string]*index.Document, error) {
	f.docs = make(map[string]*index.Document)
	f.ids = make(map[string]int)

	if err := f.documents(ctx, nil); err != nil {
		return nil, err
	}
	if err := f.tags(ctx); err != nil {
		return nil, err
	}
	if err := f.links(ctx); err != nil {
		return nil, err
	}
	if err := f.authors(ctx); err != nil {
		return nil, err
	}

	return f.docs, nil
}

func (f *Fill) document(ctx context.Context) error {
	var title sql.NullString
	var dateEpoch sql.NullInt64
	var fileTimeEpoch sql.NullInt64
	var headings sql.NullString
	var meta sql.NullString

	row := f.Db.QueryRowContext(ctx, `
	SELECT id, title, date, fileTime, headings, meta
	FROM Documents
	WHERE path = ?
	`, f.Path)
	if err := row.Scan(&f.id, &title, &dateEpoch, &fileTimeEpoch, &headings, &meta); err != nil {
		return err
	}

	if title.Valid {
		f.doc.Title = title.String
	}
	if dateEpoch.Valid {
		f.doc.Date = time.Unix(dateEpoch.Int64, 0)
	}
	if fileTimeEpoch.Valid {
		f.doc.FileTime = time.Unix(fileTimeEpoch.Int64, 0)
	}
	if headings.Valid {
		f.doc.Headings = headings.String
	}
	if meta.Valid {
		f.doc.OtherMeta = meta.String
	}
	return nil
}

// Fill document info for documents provided by rows (id, path, title, date, fileTime, meta)
// pass nil rows to get all documents in the database.
func (f *FillMany) documents(ctx context.Context, rows *sql.Rows) error {
	if rows == nil {
		var err error
		rows, err = f.Db.QueryContext(ctx, `
	SELECT id, path, title, date, fileTime, headings, meta
	FROM Documents
	`)
		if err != nil {
			return err
		}
		defer rows.Close()
	} else if cols, err := rows.ColumnTypes(); err != nil {
		return err
	} else if len(cols) != 7 {
		return fmt.Errorf("Not enough columns to fill documents with")
	} else if t := cols[0].DatabaseTypeName(); t != "INTEGER" {
		return fmt.Errorf("Expected integer for id column fill, got %s", t)
	} else if t := cols[1].DatabaseTypeName(); t != "TEXT" {
		return fmt.Errorf("Expected text for path column fill, got %s", t)
	} else if t := cols[2].DatabaseTypeName(); t != "TEXT" {
		return fmt.Errorf("Expected text for title column fill, got %s", t)
	} else if t := cols[3].DatabaseTypeName(); t != "INT" {
		return fmt.Errorf("Expected integer for date column fill, got %s", t)
	} else if t := cols[4].DatabaseTypeName(); t != "INT" {
		return fmt.Errorf("Expected integer for filetime column fill, got %s", t)
	} else if t := cols[5].DatabaseTypeName(); t != "TEXT" {
		return fmt.Errorf("Expected text for headings column fill, got %s", t)
	} else if t := cols[6].DatabaseTypeName(); t != "BLOB" {
		return fmt.Errorf("Expected text for meta column fill, got %s", t)
	}

	var id int
	var docPath string
	var title, headings, meta sql.NullString
	var dateEpoch, filetimeEpoch sql.NullInt64

	for rows.Next() {
		if err := rows.Scan(&id, &docPath, &title, &dateEpoch, &filetimeEpoch, &headings, &meta); err != nil {
			return err
		}

		doc := &index.Document{
			Path: docPath,
		}

		if title.Valid {
			doc.Title = title.String
		}
		if dateEpoch.Valid {
			doc.Date = time.Unix(dateEpoch.Int64, 0)
		}
		if filetimeEpoch.Valid {
			doc.FileTime = time.Unix(filetimeEpoch.Int64, 0)
		}
		if headings.Valid {
			doc.Headings = headings.String
		}
		if meta.Valid {
			doc.OtherMeta = meta.String
		}

		f.docs[docPath] = doc
		f.ids[docPath] = id
	}

	return nil
}
func (f Fill) authors(ctx context.Context) error {
	rows, err := f.Db.QueryContext(ctx, `
	SELECT name
	FROM Authors
	JOIN DocumentAuthors
	ON Authors.id = DocumentAuthors.authorId
	WHERE docId = ?
	`, f.id)
	if err != nil {
		return err
	}
	defer rows.Close()

	var name string
	authors := make([]string, 0, 4)
	for rows.Next() {
		if err := rows.Scan(&name); err != nil {
			return err
		}
		authors = append(authors, name)
	}

	f.doc.Authors = authors

	return nil
}

func (f FillMany) authors(ctx context.Context) error {
	stmt, err := f.Db.PrepareContext(ctx, `
	SELECT name
	FROM Authors
	JOIN DocumentAuthors
	ON Authors.id = DocumentAuthors.authorId
	WHERE docId = ?
	`)
	if err != nil {
		return err
	}
	defer stmt.Close()

	// PERF: parallelize
	var name string
	for path, id := range f.ids {
		rows, err := stmt.QueryContext(ctx, id)
		if err != nil {
			return err
		}

		doc := f.docs[path]
		for rows.Next() {
			if err := rows.Scan(&name); err != nil {
				rows.Close()
				return err
			}

			doc.Authors = append(doc.Authors, name)
		}

		rows.Close()
	}

	return nil
}

func (f Fill) tags(ctx context.Context) error {
	rows, err := f.Db.QueryContext(ctx, `
	SELECT name
	FROM Tags
	JOIN DocumentTags
	ON Tags.id = DocumentTags.tagId
	WHERE docId = ?
	`, f.id)
	if err != nil {
		panic(err)
	}
	defer rows.Close()

	var tag string
	tags := make([]string, 0, 2)
	for rows.Next() {
		if err := rows.Scan(&tag); err != nil {
			return err
		}
		tags = append(tags, tag)
	}

	f.doc.Tags = tags

	return nil
}

func (f FillMany) tags(ctx context.Context) error {
	stmt, err := f.Db.PrepareContext(ctx, `
	SELECT name
	FROM Tags
	JOIN DocumentTags
	ON Tags.id = DocumentTags.tagId
	WHERE docId = ?
	`)
	if err != nil {
		return err
	}
	defer stmt.Close()

	// PERF: parallelize
	var tag string
	for docPath, id := range f.ids {
		rows, err := stmt.QueryContext(ctx, id)
		if err != nil {
			return err
		}

		doc := f.docs[docPath]
		for rows.Next() {
			if err := rows.Scan(&tag); err != nil {
				rows.Close()
				return err
			}

			doc.Tags = append(doc.Tags, tag)
		}

		rows.Close()
	}

	return nil
}

func (f Fill) links(ctx context.Context) error {
	rows, err := f.Db.QueryContext(ctx, `
	SELECT link
	FROM Links
	WHERE Links.docId = ?
	`, f.id)
	if err != nil {
		return err
	}
	defer rows.Close()

	var link string
	links := make([]string, 0)
	for rows.Next() {
		if err := rows.Scan(&link); err != nil {
			return err
		}
		links = append(links, link)
	}
	f.doc.Links = links

	return nil
}

func (f FillMany) links(ctx context.Context) error {
	stmt, err := f.Db.PrepareContext(ctx, `
	SELECT link
	FROM Links
	WHERE Links.docId = ?
	`)
	if err != nil {
		return err
	}
	defer stmt.Close()

	// PERF: parallelize
	var linkPath string
	for path, id := range f.ids {
		rows, err := stmt.QueryContext(ctx, id)
		if err != nil {
			return err
		}

		doc := f.docs[path]
		for rows.Next() {
			if err := rows.Scan(&linkPath); err != nil {
				rows.Close()
				return err
			}
			doc.Links = append(doc.Links, linkPath)
		}

		rows.Close()
	}

	return nil
}
