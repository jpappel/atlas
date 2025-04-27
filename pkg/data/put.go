package data

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/jpappel/atlas/pkg/index"
)

// TODO: rename struct
type Put struct {
	Id  int64
	Doc index.Document
	tx  *sql.Tx
}

// TODO: rename struct
type PutMany struct {
	Docs     map[int64]*index.Document
	pathDocs map[string]*index.Document
	db       *sql.DB
}

func NewPut(ctx context.Context, db *sql.DB, doc index.Document) (Put, error) {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return Put{}, nil
	}
	p := Put{Doc: doc, tx: tx}
	return p, nil
}

func NewPutMany(db *sql.DB, documents map[string]*index.Document) (PutMany, error) {
	docs := make(map[int64]*index.Document, len(documents))
	p := PutMany{
		Docs:     docs,
		pathDocs: documents,
		db:       db,
	}
	return p, nil
}

func (p Put) Insert() error {
	if err := p.document(); err != nil {
		p.tx.Rollback()
		return err
	}

	if err := p.tags(); err != nil {
		p.tx.Rollback()
		return err
	}

	if err := p.links(); err != nil {
		p.tx.Rollback()
		return err
	}

	if err := p.authors(); err != nil {
		p.tx.Rollback()
		return err
	}

	return p.tx.Commit()
}

func (p PutMany) Insert(ctx context.Context) error {
	if err := p.documents(ctx); err != nil {
		return err
	}

	if err := p.tags(ctx); err != nil {
		return err
	}

	if err := p.links(ctx); err != nil {
		return err
	}

	if err := p.authors(ctx); err != nil {
		return err
	}

	return nil
}

func (p *Put) document() error {
	title := sql.NullString{String: p.Doc.Title, Valid: p.Doc.Title != ""}

	dateUnix := p.Doc.Date.Unix()
	date := sql.NullInt64{Int64: dateUnix, Valid: dateUnix != 0}

	filetimeUnix := p.Doc.FileTime.Unix()
	filetime := sql.NullInt64{Int64: filetimeUnix, Valid: filetimeUnix != 0}

	meta := sql.NullString{String: p.Doc.OtherMeta, Valid: p.Doc.OtherMeta != ""}

	result, err := p.tx.Exec(`
	INSERT INTO Documents(path, title, date, fileTime, meta)
	VALUES (?,?,?,?,?)
	`, p.Doc.Path, title, date, filetime, meta)
	if err != nil {
		return err
	}

	p.Id, err = result.LastInsertId()
	if err != nil {
		return err
	}

	return nil
}

func (p *PutMany) documents(ctx context.Context) error {
	stmt, err := p.db.PrepareContext(ctx, `
	INSERT INTO Documents(path, title, date, fileTime, meta)
	VALUES (?,?,?,?,?)
	`)
	if err != nil {
		return err
	}
	defer stmt.Close()

	tx, err := p.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}

	txStmt := tx.StmtContext(ctx, stmt)

	// PERF: profile this, grabbing the docId here might save time by simpliyfying
	//       future inserts
	for _, doc := range p.pathDocs {
		title := sql.NullString{String: doc.Title, Valid: doc.Title != ""}
		dateUnix := doc.Date.Unix()
		date := sql.NullInt64{Int64: dateUnix, Valid: dateUnix != 0}

		filetimeUnix := doc.FileTime.Unix()
		filetime := sql.NullInt64{Int64: filetimeUnix, Valid: filetimeUnix != 0}

		meta := sql.NullString{String: doc.OtherMeta, Valid: doc.OtherMeta != ""}

		res, err := txStmt.ExecContext(ctx, doc.Path, title, date, filetime, meta)
		if err != nil {
			tx.Rollback()
			return err
		}

		id, err := res.LastInsertId()
		if err != nil {
			tx.Rollback()
			return err
		}

		p.Docs[id] = doc
	}

	return tx.Commit()
}

func (p Put) tags() error {
	query, args := BatchQuery("INSERT OR IGNORE INTO Tags (name) VALUES", "", "(?)", ",", "", len(p.Doc.Tags), p.Doc.Tags)
	if _, err := p.tx.Exec(query, args...); err != nil {
		return err
	}

	preQuery := fmt.Sprintf(`
	INSERT INTO DocumentTags
		SELECT %d, Tags.id
		FROM Tags
		WHERE name IN
	`, p.Id)

	query, args = BatchQuery(preQuery, "(", "?", ",", ")", len(p.Doc.Tags), p.Doc.Tags)
	if _, err := p.tx.Exec(query, args...); err != nil {
		return err
	}

	return nil
}

func (p PutMany) tags(ctx context.Context) error {
	newTagStmt, err := p.db.PrepareContext(ctx, "INSERT OR IGNORE INTO Tags (name) VALUES (?)")
	if err != nil {
		return err
	}
	defer newTagStmt.Close()

	tx, err := p.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}

	txNewTagStmt := tx.StmtContext(ctx, newTagStmt)

	for id, doc := range p.Docs {
		for _, tag := range doc.Tags {
			if _, err := txNewTagStmt.ExecContext(ctx, tag); err != nil {
				tx.Rollback()
				return err
			}
		}

		preQuery := fmt.Sprintf(`
		INSERT INTO DocumentTags (docId, tagId)
			SELECT %d, Tags.id
			FROM Tags
			WHERE name IN
		`, id)
		query, args := BatchQuery(preQuery, "(", "?", ",", ")", len(doc.Tags), doc.Tags)
		if _, err := tx.Exec(query, args); err != nil {
			tx.Rollback()
			return err
		}
	}

	return tx.Commit()
}

func (p Put) links() error {
	if len(p.Doc.Links) == 0 {
		return nil
	}

	preQuery := fmt.Sprintf(`
		INSERT INTO Links (referencedId, refererId)
			SELECT id, %d
			FROM Documents
			WHERE path IN
	`, p.Id)
	query, args := BatchQuery(preQuery, "(", "?", ",", ")", len(p.Doc.Links), p.Doc.Links)
	if _, err := p.tx.Exec(query, args...); err != nil {
		return err
	}

	return nil
}

func (p PutMany) links(ctx context.Context) error {
	tx, err := p.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}

	for id, doc := range p.Docs {
		preQuery := fmt.Sprintf(`
		INSERT INTO Links (referencedId, refererId)
			SELECT id, %d
			FROM Documents
			WHERE path IN
	`, id)
		query, args := BatchQuery(preQuery, "(", "?", ",", ")", len(doc.Links), doc.Links)
		if _, err := tx.Exec(query, args...); err != nil {
			tx.Rollback()
			return err
		}
	}

	return tx.Commit()
}

func (p Put) authors() error {
	// TODO: consider using temp table instead of cte
	namesCTE, args := BatchQuery("WITH names(n) AS",
		"( VALUES ", "(?)", ",", "),", len(p.Doc.Authors), p.Doc.Authors)

	newAuthorsQuery := namesCTE + `
	filtered_names AS (
		SELECT n
		FROM names
		LEFT JOIN Authors on Authors.name = n
		LEFT JOIN Aliases on Aliases.alias = n
		WHERE Authors.name IS NULL AND Aliases.alias IS NULL
	)
	INSERT INTO Authors(name)
	SELECT n FROM filtered_names
	`
	if _, err := p.tx.Exec(newAuthorsQuery, args...); err != nil {
		return err
	}

	docAuthorsQuery := namesCTE + fmt.Sprintf(`
	matched_authors AS (
		SELECT Authors.id AS author_id
		FROM Authors
		LEFT JOIN Aliases
		ON Authors.id = Aliases.authorId
		JOIN names
		ON Authors.name = n OR Aliases.alias = n
	)
	INSERT INTO DocumentAuthors(docId, authorId)
	SELECT %d, author_id FROM matched_authors
	`, p.Id)
	if _, err := p.tx.Exec(docAuthorsQuery, args...); err != nil {
		return err
	}

	return nil
}

func (p PutMany) authors(ctx context.Context) error {
	tx, err := p.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}

	_, err = p.db.Exec("CREATE TEMPORARY TABLE names (name TEXT UNIQUE NOT NULL)")
	// _, err = tx.Exec("CREATE TEMPORARY TABLE names (name TEXT UNIQUE NOT NULL)")
	if err != nil {
		tx.Rollback()
		return err
	}
	defer p.db.Exec("DROP TABLE IF EXISTS temp.names")

	nameStmt, err := p.db.PrepareContext(ctx, "INSERT OR IGNORE INTO temp.names VALUES (?)")
	if err != nil {
		return err
	}
	defer nameStmt.Close()

	txNameStmt := tx.StmtContext(ctx, nameStmt)
	for _, doc := range p.Docs {
		for _, name := range doc.Authors {
			if _, err := txNameStmt.Exec(name); err != nil {
				tx.Rollback()
				return err
			}
		}
	}

	newAuthorsQuery := `
	WITH new_names AS (
		SELECT name
		FROM temp.names
		LEFT JOIN Authors on Authors.name = temp.names.name
		LEFT JOIN Aliases on Aliases.alias = tmep.names.name
		WHERE Authors.name IS NULL AND Aliases.alias IS NULL
	)
	INSERT INTO Authors(name)
	SELECT name FROM new_names
	`

	if _, err := tx.Exec(newAuthorsQuery); err != nil {
		tx.Rollback()
		return err
	}

	_, err = tx.Exec(`
	CREATE TEMPORARY TABLE name_ids AS
		SELECT names.name AS name, COALESCE(Authors.id, Aliases.authorId) AS authorId
		FROM names
		LEFT JOIN Authors ON temp.names.name = Authors.name
		LEFT JOIN Aliases ON temp.names.name = Aliases.name
	`)
	if err != nil {
		tx.Rollback()
		return err
	}
	defer p.db.Exec("DROP TABLE IF EXISTS temp.name_ids")

	docAuthorsStmt, err := p.db.Prepare(`
	INSERT INTO DocumentAuthors (docId, authorId)
	SELECT ?, authorId
	FROM temp.name_ids
	WHERE name = ?
	`)
	if err != nil {
		tx.Rollback()
		return err
	}
	defer docAuthorsStmt.Close()

	for id, doc := range p.Docs {
		for _, name := range doc.Authors {
			if _, err := tx.Stmt(docAuthorsStmt).Exec(id, name); err != nil {
				tx.Rollback()
				return err
			}
		}
	}

	return tx.Commit()
}
