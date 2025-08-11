package data

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"regexp"
	"strings"
	"time"

	"github.com/jpappel/atlas/pkg/index"
	"github.com/jpappel/atlas/pkg/query"
	"github.com/mattn/go-sqlite3"
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

func NewQuery(filename string, version string) *Query {
	query := &Query{NewDB(filename, version)}
	return query
}

func NewDB(filename string, version string) *sql.DB {
	connStr := "file:" + filename + "?_fk=true&_journal=WAL"
	db, err := sql.Open("sqlite3_regex", connStr)
	if err != nil {
		panic(err)
	}

	var dbVersion string
	row := db.QueryRow("SELECT key, value FROM Info WHERE key='version'")
	if err := row.Scan(&dbVersion); err == nil {
		return db
	}

	if err := createSchema(db, version); err != nil {
		panic(err)
	}

	return db
}

func NewMemDB(version string) *sql.DB {
	db, err := sql.Open("sqlite3_regex", ":memory:?_fk=true")
	if err != nil {
		panic(err)
	}

	if err := createSchema(db, version); err != nil {
		panic(err)
	}

	return db
}

func createSchema(db *sql.DB, version string) error {
	tx, err := db.Begin()
	if err != nil {
		return err
	}

	_, err = tx.Exec(`
	CREATE TABLE IF NOT EXISTS Info(
		key TEXT PRIMARY KEY NOT NULL,
		value TEXT NOT NULL,
		updated INT NOT NULL
	)`)
	if err != nil {
		tx.Rollback()
		return err
	}

	_, err = tx.Exec(`
	CREATE TABLE IF NOT EXISTS Documents(
		id INTEGER PRIMARY KEY,
		path TEXT UNIQUE NOT NULL,
		headings TEXT,
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
		author TEXT UNIQUE NOT NULL
	)`)
	if err != nil {
		tx.Rollback()
		return err
	}

	_, err = tx.Exec(`
	CREATE TABLE IF NOT EXISTS Tags(
		id INTEGER PRIMARY KEY,
		tag TEXT UNIQUE NOT NULL
	)`)
	if err != nil {
		tx.Rollback()
		return err
	}

	_, err = tx.Exec(`
	CREATE TABLE IF NOT EXISTS Links(
		docId INT,
		link TEXT NOT NULL,
		FOREIGN KEY (docId) REFERENCES Documents(id) ON DELETE CASCADE,
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
		FOREIGN KEY (docId) REFERENCES Documents(id) ON DELETE CASCADE,
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
		FOREIGN KEY (docId) REFERENCES Documents(id) ON DELETE CASCADE,
		FOREIGN KEY (tagId) REFERENCES Tags(id),
		UNIQUE(docId, tagId)
	)`)
	if err != nil {
		tx.Rollback()
		return err
	}

	_, err = tx.Exec("CREATE INDEX IF NOT EXISTS idx_doc_paths ON Documents (path)")
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
	CREATE VIRTUAL TABLE IF NOT EXISTS Documents_fts
	USING fts5 (
		path, headings, title, meta, content=Documents, content_rowid=id, tokenize="trigram"
	)
	`)
	if err != nil {
		tx.Rollback()
		return err
	}

	_, err = tx.Exec(`
	CREATE VIRTUAL TABLE IF NOT EXISTS Authors_fts
	USING fts5 (
		author, content=Authors, content_rowid=id, tokenize="trigram"
	)
	`)
	if err != nil {
		tx.Rollback()
		return err
	}

	_, err = tx.Exec(`
	CREATE VIRTUAL TABLE IF NOT EXISTS Tags_fts
	USING fts5 (
		tag, content=Tags, content_rowid=id, tokenize="trigram"
	)
	`)
	if err != nil {
		tx.Rollback()
		return err
	}

	_, err = tx.Exec(`
	CREATE VIRTUAL TABLE IF NOT EXISTS Links_fts
	USING fts5 (
		link, docId UNINDEXED,content=Links, tokenize="trigram"
	)
	`)

	// FIXME: doesn't set new.id
	_, err = tx.Exec(`
	CREATE TRIGGER IF NOT EXISTS trig_ai_authors
	AFTER INSERT ON Authors
	BEGIN
		INSERT INTO Authors_fts(rowid, author)
		VALUES (new.id, new.author);
	END
	`)
	if err != nil {
		tx.Rollback()
		return err
	}

	_, err = tx.Exec(`
	CREATE TRIGGER IF NOT EXISTS trig_ad_authors
	AFTER DELETE ON Authors
	BEGIN
		INSERT INTO Authors_fts(Authors_fts, rowid, author)
		VALUES ('delete', old.id, old.author);
	END
	`)
	if err != nil {
		tx.Rollback()
		return err
	}

	// FIXME: doesn't set new.id
	_, err = tx.Exec(`
	CREATE TRIGGER IF NOT EXISTS trig_au_authors
	AFTER UPDATE ON Authors
	BEGIN
		INSERT INTO Authors_fts(Authors_fts, rowid, author)
		VALUES ('delete', old.id, old.author);
		INSERT INTO Authors_fts(rowid, author)
		VALUES (new.id, new.author);
	END
	`)
	if err != nil {
		tx.Rollback()
		return err
	}

	_, err = tx.Exec(`
	CREATE TRIGGER IF NOT EXISTS trig_ai_tags
	AFTER INSERT ON Tags
	BEGIN
		INSERT INTO Tags_fts(rowid, tag)
		VALUES (new.id, new.tag);
	END
	`)
	if err != nil {
		tx.Rollback()
		return err
	}

	_, err = tx.Exec(`
	CREATE TRIGGER IF NOT EXISTS trig_ad_tags
	AFTER DELETE ON Tags
	BEGIN
		INSERT INTO Tags_fts(Tags_fts, rowid, tag)
		VALUES ('delete', old.id, old.tag);
	END
	`)
	if err != nil {
		tx.Rollback()
		return err
	}

	// FIXME: doesn't set new.id
	_, err = tx.Exec(`
	CREATE TRIGGER IF NOT EXISTS trig_au_tags
	AFTER UPDATE ON Tags
	BEGIN
		INSERT INTO Tags_fts(Tags_fts, rowid, tag)
		VALUES ('delete', old.id, old.tag);
		INSERT INTO Tags_fts(rowid, tag)
		VALUES (new.id, new.tag);
	END
	`)
	if err != nil {
		tx.Rollback()
		return err
	}

	_, err = tx.Exec(`
	CREATE TRIGGER IF NOT EXISTS trig_ai_links
	AFTER INSERT ON Links
	BEGIN
		INSERT INTO Links_fts(rowid, link, docId)
		VALUES (new.rowid, new.link, new.docId);
	END
	`)
	if err != nil {
		tx.Rollback()
		return err
	}

	_, err = tx.Exec(`
	CREATE TRIGGER IF NOT EXISTS trig_ad_links
	AFTER DELETE ON Links
	BEGIN
		INSERT INTO Links_fts(Links_fts, rowid, link, docId)
		VALUES ('delete', old.rowid, old.link, old.docId);
	END
	`)
	if err != nil {
		tx.Rollback()
		return err
	}

	// FIXME: doesn't set new.id
	_, err = tx.Exec(`
	CREATE TRIGGER IF NOT EXISTS trig_au_links
	AFTER UPDATE ON Links
	BEGIN
		INSERT INTO Links_fts(Links_fts, rowid, link, docId)
		VALUES ('delete', old.rowid, old.link, old.docId);
		INSERT INTO Links_fts(rowid, link, docId)
		VALUES (new.rowid, new.link, new.docId);
	END
	`)
	if err != nil {
		tx.Rollback()
		return err
	}

	// FIXME: doesn't set new.id
	_, err = tx.Exec(`
	CREATE TRIGGER IF NOT EXISTS trig_ai_doc
	AFTER INSERT ON Documents
	BEGIN
		INSERT INTO Documents_fts(rowid, path, headings, title, meta)
		VALUES (new.id, new.path, new.headings, new.title, new.meta);
	END
	`)
	if err != nil {
		tx.Rollback()
		return err
	}

	_, err = tx.Exec(`
	CREATE TRIGGER IF NOT EXISTS trig_ad_doc
	AFTER DELETE ON Documents
	BEGIN
		INSERT INTO Documents_fts(Documents_fts, rowid, path, headings, title, meta)
		VALUES ('delete', old.id, old.path, old.headings, old.title, old.meta);
	END
	`)
	if err != nil {
		tx.Rollback()
		return err
	}

	// FIXME: doesn't set new.id
	_, err = tx.Exec(`
	CREATE TRIGGER IF NOT EXISTS trig_au_doc
	AFTER UPDATE ON Documents
	BEGIN
		INSERT INTO Documents_fts(Documents_fts, rowid, path, headings, title, meta)
		VALUES ('delete', old.id, old.path, old.headings, old.title, old.meta);
		INSERT INTO Documents_fts(rowid, path, headings, title, meta)
		VALUES (new.id, new.path, new.headings, new.title, new.meta);
	END
	`)
	if err != nil {
		return err
	}

	_, err = tx.Exec(`
	CREATE VIEW IF NOT EXISTS Search AS
	SELECT
		d.id AS docId,
		d_fts.path,
		d_fts.title,
		d.date,
		d.fileTime,
		d_fts.headings,
		d_fts.meta,
		a_fts.author,
		t_fts.tag,
		l_fts.link
	FROM Documents d
	JOIN Documents_fts as d_fts ON d.id = d_fts.rowid
	LEFT JOIN DocumentAuthors da ON d.id = da.docId
	LEFT JOIN Authors_fts a_fts ON da.authorId = a_fts.rowid
	LEFT JOIN DocumentTags dt ON d.id = dt.docId
	LEFT JOIN Tags_fts t_fts ON dt.tagId = t_fts.rowid
	LEFT JOIN Links_fts l_fts ON d.id = l_fts.docId
	`)
	if err != nil {
		tx.Rollback()
		return err
	}

	if _, err = tx.Exec("PRAGMA OPTIMIZE"); err != nil {
		tx.Rollback()
		return err
	}

	t := time.Now().UTC().Unix()
	if _, err = tx.Exec("INSERT OR IGNORE INTO Info (key, value, updated) VALUES (?,?,?), (?,?,?)",
		"created", "", t,
		"version", version, t,
	); err != nil {
		tx.Rollback()
		return err
	}

	return tx.Commit()
}

func (q Query) Close() error {
	q.db.Exec("PRAGMA OPTIMIZE")
	return q.db.Close()
}

// Create an index
func (q Query) Get(ctx context.Context, indexRoot string) (*index.Index, error) {
	f := FillMany{Db: q.db}
	docs, err := f.Get(ctx)
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
func (q Query) Put(ctx context.Context, idx index.Index) error {
	p, err := NewPutMany(ctx, q.db, idx.Documents)
	if err != nil {
		return err
	}

	return p.Insert()
}

// Update database with values from index, removes entries for deleted files
func (q Query) Update(ctx context.Context, idx index.Index) error {
	u := UpdateMany{Db: q.db, PathDocs: idx.Documents}
	return u.Update(ctx)
}

func (q Query) GetDocument(ctx context.Context, path string) (*index.Document, error) {
	f := Fill{Path: path, Db: q.db}
	return f.Get(ctx)
}

// Shrink database by removing unused authors and tags and VACUUM-ing
func (q Query) Tidy() error {
	if _, err := q.db.Exec(`
	DELETE FROM Authors
	WHERE id NOT IN (
		SELECT authorId FROM DocumentAuthors
	)`); err != nil {
		return err
	}

	if _, err := q.db.Exec(`
	DELETE FROM Tags
	WHERE id NOT IN (
		SELECT tagId FROM DocumentTags
	)
	`); err != nil {
		return err
	}

	if _, err := q.db.Exec("VACUUM"); err != nil {
		return err
	}

	if _, err := q.db.Exec("INSERT INTO Documents_fts(Documents_fts) VALUES('optimize')"); err != nil {
		return err
	}
	if _, err := q.db.Exec("INSERT INTO Authors_fts(Authors_fts) VALUES('optimize')"); err != nil {
		return err
	}
	if _, err := q.db.Exec("INSERT INTO Tags_fts(Tags_fts) VALUES('optimize')"); err != nil {
		return err
	}

	return nil
}

func (q Query) PeriodicOptimize(ctx context.Context, d time.Duration) {
	_, err := q.db.ExecContext(ctx, "PRAGMA OPTIMIZE optimize=0x10002")
	if err != nil {
		return
	}

	ticker := time.NewTicker(d)

	for {
		select {
		case <-ticker.C:
			slog.Debug("Running periodic db optimization",
				slog.Int64("next", time.Now().Unix()+int64(d)),
			)
			if _, err := q.db.ExecContext(ctx, "PRAGMA OPTIMIZE"); err != nil {
				return
			}
		case <-ctx.Done():
			return
		}
	}
}

func (q Query) Execute(ctx context.Context, artifact query.CompilationArtifact) (map[string]*index.Document, error) {
	f := FillMany{
		Db:   q.db,
		docs: make(map[string]*index.Document),
		ids:  make(map[string]int),
	}

	compiledQuery := fmt.Sprintf(`
	SELECT id, d.path, d.title, d.date, d.fileTime, d.headings, d.meta
	FROM Documents d
	JOIN (
		SELECT DISTINCT docId
		FROM Search
		WHERE %s
	) s
	ON d.id = s.docId
	`, artifact.Query)

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

func regex(re, s string) (bool, error) {
	return regexp.MatchString(re, s)
}

func init() {
	sql.Register("sqlite3_regex",
		&sqlite3.SQLiteDriver{
			ConnectHook: func(sc *sqlite3.SQLiteConn) error {
				return sc.RegisterFunc("regexp", regex, true)
			},
		},
	)
}
