package data

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/jpappel/atlas/pkg/index"
	"github.com/jpappel/atlas/pkg/query"
	_ "github.com/mattn/go-sqlite3"
)

type Query struct {
	db *sql.DB
}

// Append n copies of val to query
//
// output is in the form
//
// <query> <start><(n-1)*(<val><delim)>><val><stop>
func BatchQuery[T any](query string, start string, val string, delim string, stop string, n int, baseArgs []T) (string, []any) {
	args := make([]any, len(baseArgs))
	for i, arg := range baseArgs {
		args[i] = arg
	}

	b := strings.Builder{}
	b.Grow(len(query) + 1 + len(start) + n*len(val) + ((n - 1) * len(delim)) + len(stop))

	b.WriteString(query)
	b.WriteRune(' ')
	b.WriteString(start)
	for range n - 1 {
		b.WriteString(val)
		b.WriteString(delim)
	}
	b.WriteString(val)
	b.WriteString(stop)

	return b.String(), args
}

func NewQuery(filename string) *Query {
	query := &Query{NewDB(filename)}
	return query
}

func NewDB(filename string) *sql.DB {
	connStr := "file:" + filename + "?_fk=true&_journal=WAL"
	db, err := sql.Open("sqlite3", connStr)
	if err != nil {
		panic(err)
	}

	if err := createSchema(db); err != nil {
		panic(err)
	}

	return db
}

func NewMemDB() *sql.DB {
	db, err := sql.Open("sqlite3", ":memory:?_fk=true")
	if err != nil {
		panic(err)
	}

	if err := createSchema(db); err != nil {
		panic(err)
	}

	return db
}

func createSchema(db *sql.DB) error {
	ctx := context.TODO()
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Commit()

	_, err = tx.Exec(`
	CREATE TABLE IF NOT EXISTS Indexes(
		root TEXT NOT NULL,
		followSym DATE
	)`)
	if err != nil {
		tx.Rollback()
		return err
	}

	_, err = tx.Exec(`
	CREATE TABLE IF NOT EXISTS Documents(
		id INTEGER PRIMARY KEY,
		path TEXT UNIQUE NOT NULL,
		title TEXT,
		date INT,
		fileTime INT,
		meta BLOB
	)`)
	if err != nil {
		tx.Rollback()
		return err
	}

	_, err = tx.Exec(`
	CREATE TABLE IF NOT EXISTS Authors(
		id INTEGER PRIMARY KEY,
		name TEXT UNIQUE NOT NULL
	)`)
	if err != nil {
		tx.Rollback()
		return err
	}

	_, err = tx.Exec(`
	CREATE TABLE IF NOT EXISTS Aliases(
		authorId INT NOT NULL,
		alias TEXT UNIQUE NOT NULL,
		FOREIGN KEY (authorId) REFERENCES Authors(id)
	)`)
	if err != nil {
		tx.Rollback()
		return err
	}

	_, err = tx.Exec(`
	CREATE TABLE IF NOT EXISTS Tags(
		id INTEGER PRIMARY KEY,
		name TEXT UNIQUE NOT NULL
	)`)
	if err != nil {
		tx.Rollback()
		return err
	}

	_, err = tx.Exec(`
	CREATE TABLE IF NOT EXISTS Links(
		docId INT,
		link TEXT NOT NULL,
		FOREIGN KEY (docId) REFERENCES Documents(id),
		UNIQUE(docId, link)
	)`)
	if err != nil {
		tx.Rollback()
		return err
	}

	_, err = tx.Exec(`
	CREATE TABLE IF NOT EXISTS DocumentAuthors(
		docId INT NOT NULL,
		authorId INT NOT NULL,
		FOREIGN KEY (docId) REFERENCES Documents(id),
		FOREIGN KEY (authorId) REFERENCES Authors(id)
	)`)
	if err != nil {
		tx.Rollback()
		return err
	}

	_, err = tx.Exec(`
	CREATE TABLE IF NOT EXISTS DocumentTags(
		docId INT NOT NULL,
		tagId INT NOT NULL,
		FOREIGN KEY (docId) REFERENCES Documents(id),
		FOREIGN KEY (tagId) REFERENCES Tags(id),
		UNIQUE(docId, tagId)
	)`)
	if err != nil {
		tx.Rollback()
		return err
	}

	_, err = tx.Exec("CREATE INDEX IF NOT EXISTS idx_doc_dates ON Documents (date)")
	if err != nil {
		tx.Rollback()
		return err
	}

	_, err = tx.Exec("CREATE INDEX IF NOT EXISTS idx_doc_titles ON Documents (title)")
	if err != nil {
		tx.Rollback()
		return err
	}

	_, err = tx.Exec("CREATE INDEX IF NOT EXISTS idx_aliases_alias ON Aliases(alias)")
	if err != nil {
		tx.Rollback()
		return err
	}

	_, err = tx.Exec("CREATE INDEX IF NOT EXISTS idx_aliases_authorId ON Aliases(authorId)")
	if err != nil {
		tx.Rollback()
		return err
	}

	_, err = tx.Exec("CREATE INDEX IF NOT EXISTS idx_links_link ON Links(link)")
	if err != nil {
		tx.Rollback()
		return err
	}

	_, err = tx.Exec("CREATE INDEX IF NOT EXISTS idx_doctags_tagid ON DocumentTags (tagId)")
	if err != nil {
		tx.Rollback()
		return err
	}

	_, err = tx.Exec(`
	CREATE VIEW IF NOT EXISTS Search AS
	SELECT
		d.id AS docId,
		d.path,
		d.title,
		d.date,
		d.fileTime,
		d.meta,
		COALESCE(a.name, al.alias) AS author,
		t.name AS tag,
		l.link
	FROM Documents d
	LEFT JOIN DocumentAuthors da ON d.id = da.docId
	LEFT JOIN Authors a ON da.authorId = a.id
	LEFT JOIN Aliases al ON a.id = al.authorId
	LEFT JOIN DocumentTags dt ON d.id = dt.docId
	LEFT JOIN Tags t ON dt.tagId = t.id
	LEFT JOIN Links l ON d.id = l.docId
	`)
	if err != nil {
		tx.Rollback()
		return err
	}

	return nil
}

func (q Query) Close() error {
	return q.db.Close()
}

// Create an index
func (q Query) Get(indexRoot string) (*index.Index, error) {
	ctx := context.TODO()

	docs, err := FillMany{Db: q.db}.Get(ctx)
	if err != nil {
		return nil, err
	}

	idx := &index.Index{
		Root:      indexRoot,
		Documents: docs,
		Filters:   index.DefaultFilters(),
	}

	return idx, nil
}

// Write from index to database
func (q Query) Put(idx index.Index) error {
	ctx := context.TODO()

	p, err := NewPutMany(q.db, idx.Documents)
	if err != nil {
		return err
	}

	if err := p.Insert(ctx); err != nil {
		return err
	}

	return nil
}

// Update database with values from index
func (q Query) Update(idx index.Index) error {
	// TODO: implement
	return nil
}

func (q Query) GetDocument(path string) (*index.Document, error) {
	ctx := context.TODO()
	f := Fill{Path: path, Db: q.db}
	return f.Get(ctx)
}

func (q Query) Execute(artifact query.CompilationArtifact) (map[string]*index.Document, error) {
	ctx := context.TODO()
	f := FillMany{
		Db:   q.db,
		docs: make(map[string]*index.Document),
		ids:  make(map[string]int),
	}

	compiledQuery := fmt.Sprintf(`
	SELECT DISTINCT docId, path, title, date, fileTime, meta
	FROM Search
	WHERE %s`, artifact.Query)

	rows, err := q.db.QueryContext(ctx, compiledQuery, artifact.Args...)
	if err != nil {
		return nil, err
	}

	if err := f.documents(ctx, rows); err != nil {
		rows.Close()
		return nil, err
	}
	rows.Close()

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
