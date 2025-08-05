package data

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/jpappel/atlas/pkg/index"
)

type Put struct {
	Id  int64
	Doc index.Document
	tx  *sql.Tx
	db  *sql.DB
}

type PutMany struct {
	Docs     map[int64]*index.Document
	pathDocs map[string]*index.Document
	db       *sql.DB
	ctx      context.Context
}

func NewPut(db *sql.DB, doc index.Document) Put {
	return Put{Doc: doc, db: db}
}

func NewPutMany(ctx context.Context, db *sql.DB, documents map[string]*index.Document) (PutMany, error) {
	docs := make(map[int64]*index.Document, len(documents))
	p := PutMany{
		Docs:     docs,
		pathDocs: documents,
		db:       db,
		ctx:      ctx,
	}
	return p, nil
}

func (p *Put) Insert(ctx context.Context) error {
	var err error
	p.tx, err = p.db.BeginTx(ctx, nil)
	if err != nil {
		return nil
	}

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

	if _, err := p.tx.Exec("INSERT OR REPLACE INTO Info(key,value,updated) VALUES (?,?,?)",
		"lastUpdate", "singlePut", time.Now().UTC().Unix(),
	); err != nil {
		p.tx.Rollback()
		return err
	}

	return p.tx.Commit()
}

func (p PutMany) Insert() error {
	if err := p.documents(p.ctx); err != nil {
		return fmt.Errorf("failed to insert documents: %v", err)
	}

	if err := p.tags(p.ctx); err != nil {
		return fmt.Errorf("failed to insert tags: %v", err)
	}

	if err := p.links(p.ctx); err != nil {
		return fmt.Errorf("failed to insert links: %v", err)
	}

	if err := p.authors(p.ctx); err != nil {
		return fmt.Errorf("failed to insert authors: %v", err)
	}

	if _, err := p.db.ExecContext(p.ctx, "INSERT OR REPLACE INTO Info(key,value,updated) VALUES (?,?,?)",
		"lastUpdate", "multiPut", time.Now().UTC().Unix(),
	); err != nil {
		return err
	}

	return nil
}

func (p *Put) document() error {
	title := sql.NullString{String: p.Doc.Title, Valid: p.Doc.Title != ""}
	date := sql.NullInt64{Int64: p.Doc.Date.Unix(), Valid: !p.Doc.Date.IsZero()}
	filetime := sql.NullInt64{Int64: p.Doc.FileTime.Unix(), Valid: !p.Doc.FileTime.IsZero()}
	headings := sql.NullString{String: p.Doc.Headings, Valid: p.Doc.Headings != ""}
	meta := sql.NullString{String: p.Doc.OtherMeta, Valid: p.Doc.OtherMeta != ""}

	result, err := p.tx.Exec(`
	INSERT INTO Documents(path, title, date, fileTime, headings, meta)
	VALUES (?,?,?,?,?,?)
	`, p.Doc.Path, title, date, filetime, headings, meta)
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
	tx, err := p.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}

	stmt, err := tx.PrepareContext(ctx, `
	INSERT INTO Documents(path, title, date, fileTime, headings, meta)
	VALUES (?,?,?,?,?,?)
	`)
	if err != nil {
		return err
	}
	defer stmt.Close()

	// PERF: profile this, grabbing the docId here might save time by simpliyfying
	//       future inserts
	for _, doc := range p.pathDocs {
		title := sql.NullString{String: doc.Title, Valid: doc.Title != ""}
		date := sql.NullInt64{Int64: doc.Date.Unix(), Valid: !doc.Date.IsZero()}
		filetime := sql.NullInt64{Int64: doc.FileTime.Unix(), Valid: !doc.FileTime.IsZero()}
		headings := sql.NullString{String: doc.Headings, Valid: doc.Headings != ""}
		meta := sql.NullString{String: doc.OtherMeta, Valid: doc.OtherMeta != ""}

		res, err := stmt.ExecContext(ctx, doc.Path, title, date, filetime, headings, meta)
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
	if len(p.Doc.Tags) == 0 {
		return nil
	}

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
	tx, err := p.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}

	txNewTagStmt, err := tx.Prepare("INSERT OR IGNORE INTO Tags (name) VALUES (?)")
	if err != nil {
		tx.Rollback()
		return err
	}
	defer txNewTagStmt.Close()

	for id, doc := range p.Docs {
		if len(doc.Tags) == 0 {
			continue
		}
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
		if _, err := tx.Exec(query, args...); err != nil {
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

	preQuery := `
		INSERT INTO Links (docId, link)
		VALUES
	`
	valueStr := fmt.Sprintf("(%d,?)", p.Id)
	query, args := BatchQuery(preQuery, "", valueStr, ",", "", len(p.Doc.Links), p.Doc.Links)
	if _, err := p.tx.Exec(query+"\n ON CONFLICT DO NOTHING", args...); err != nil {
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
		if len(doc.Links) == 0 {
			continue
		}

		preQuery := `
		INSERT INTO Links (docId, link)
		VALUES
	`
		valueStr := fmt.Sprintf("(%d,?)", id)
		query, args := BatchQuery(preQuery, "", valueStr, ",", "", len(doc.Links), doc.Links)
		if _, err := tx.Exec(query+"\n ON CONFLICT DO NOTHING", args...); err != nil {
			tx.Rollback()
			return err
		}
	}

	return tx.Commit()
}

func (p Put) authors() error {
	if len(p.Doc.Authors) == 0 {
		return nil
	}

	// PERF: consider using temp table instead of cte
	namesCTE, args := BatchQuery("WITH names(n) AS",
		"( VALUES ", "(?)", ",", "),", len(p.Doc.Authors), p.Doc.Authors)

	newAuthorsQuery := namesCTE + `
	filtered_names AS (
		SELECT n
		FROM names
		LEFT JOIN (
			SELECT * FROM Authors
			UNION ALL
			SELECT * FROM Aliases
		) AS existing ON existing.name = names.n
		WHERE existing.name IS NULL
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
		LEFT JOIN Aliases ON Authors.id = Aliases.authorId
		JOIN names ON Authors.name = n OR Aliases.alias = n
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

	_, err = tx.Exec("CREATE TEMPORARY TABLE names (name TEXT UNIQUE NOT NULL)")
	if err != nil {
		tx.Rollback()
		return err
	}
	defer p.db.Exec("DROP TABLE IF EXISTS temp.names")

	nameStmt, err := tx.Prepare("INSERT OR IGNORE INTO temp.names VALUES (?)")
	if err != nil {
		return err
	}
	defer nameStmt.Close()

	txNameStmt := tx.StmtContext(ctx, nameStmt)
	for _, doc := range p.Docs {
		if len(doc.Authors) == 0 {
			continue
		}
		for _, name := range doc.Authors {
			if _, err := txNameStmt.Exec(name); err != nil {
				tx.Rollback()
				return err
			}
		}
	}

	newAuthorsQuery := `
	WITH new_names AS (
		SELECT temp.names.name
		FROM temp.names
		LEFT JOIN (
			SELECT * FROM Authors
			UNION ALL
			SELECT * FROM Aliases
		) AS existing ON existing.name = temp.names.name
		WHERE existing.name IS NULL
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
		SELECT names.name AS name, existing.id AS authorId
		FROM temp.names
		LEFT JOIN (
			SELECT * FROM Authors
			UNION ALL
			SELECT * FROM Aliases
		) AS existing ON existing.name = temp.names.name
	`)
	if err != nil {
		tx.Rollback()
		return err
	}
	defer p.db.Exec("DROP TABLE IF EXISTS temp.name_ids")

	docAuthorsStmt, err := tx.Prepare(`
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
		if len(doc.Authors) == 0 {
			continue
		}
		for _, name := range doc.Authors {
			if _, err := tx.Stmt(docAuthorsStmt).Exec(id, name); err != nil {
				tx.Rollback()
				return err
			}
		}
	}

	return tx.Commit()
}
