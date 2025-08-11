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

	query, args := BatchQuery("INSERT OR IGNORE INTO Tags (tag) VALUES", "", "(?)", ",", "", len(p.Doc.Tags), p.Doc.Tags)
	if _, err := p.tx.Exec(query, args...); err != nil {
		return err
	}

	preQuery := fmt.Sprintf(`
	INSERT INTO DocumentTags
		SELECT %d, Tags.id
		FROM Tags
		WHERE tag IN
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

	txNewTagStmt, err := tx.Prepare("INSERT OR IGNORE INTO Tags (tag) VALUES (?)")
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
			WHERE tag IN
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

	authStmt, err := p.tx.Prepare("INSERT OR IGNORE INTO Authors(author) VALUES(?)")
	if err != nil {
		return err
	}
	defer authStmt.Close()

	idStmt, err := p.tx.Prepare("SELECT id FROM Authors WHERE author = ?")
	if err != nil {
		return err
	}
	defer idStmt.Close()

	docAuthStmt, err := p.tx.Prepare(
		fmt.Sprintf("INSERT INTO DocumentAuthors(docId,authorId) VALUES (%d,?)", p.Id),
	)
	if err != nil {
		return err
	}
	defer docAuthStmt.Close()

	// sqlite is fast, and i'm too lazy to batch this
	var authId int64
	for _, author := range p.Doc.Authors {
		if _, err := authStmt.Exec(author); err != nil {
			return err
		}
		if err := idStmt.QueryRow(author).Scan(&authId); err != nil {
			return err
		}
		if _, err := docAuthStmt.Exec(authId); err != nil {
			return err
		}
	}

	return nil
}

func (p PutMany) authors(ctx context.Context) error {
	tx, err := p.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}

	authStmt, err := tx.Prepare("INSERT OR IGNORE INTO Authors(author) VALUES(?)")
	if err != nil {
		return err
	}
	defer authStmt.Close()

	idStmt, err := tx.Prepare("SELECT id FROM Authors WHERE author = ?")
	if err != nil {
		return err
	}
	defer idStmt.Close()

	docAuthStmt, err := tx.Prepare("INSERT INTO DocumentAuthors(docId,authorId) VALUES (?,?)")
	if err != nil {
		return err
	}
	defer docAuthStmt.Close()

	var authId int64
	for docId, doc := range p.Docs {
		for _, author := range doc.Authors {
			if _, err := authStmt.Exec(author); err != nil {
				return err
			}
			if err := idStmt.QueryRow(author).Scan(&authId); err != nil {
				return err
			}
			if _, err := docAuthStmt.Exec(docId, authId); err != nil {
				return err
			}
		}

	}

	return tx.Commit()
}
