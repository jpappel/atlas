package data

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"time"

	"github.com/jpappel/atlas/pkg/index"
)

type Update struct {
	Id  int64
	Doc index.Document
	db  *sql.DB
	tx  *sql.Tx
}

type UpdateMany struct {
	Docs     map[int64]*index.Document
	PathDocs map[string]*index.Document
	tx       *sql.Tx
	Db       *sql.DB
}

func NewUpdate(ctx context.Context, db *sql.DB, doc index.Document) Update {
	return Update{Doc: doc, db: db}
}

// Replace a document if its filetime is newer than the one in the database.
func (u *Update) Update(ctx context.Context) error {
	var err error
	u.tx, err = u.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}

	isUpdate, err := u.document()
	if !isUpdate || err != nil {
		u.tx.Rollback()
		return err
	}

	if err := u.tags(); err != nil {
		u.tx.Rollback()
		return err
	}

	if err := u.links(); err != nil {
		u.tx.Rollback()
		return err
	}

	if err := u.authors(); err != nil {
		u.tx.Rollback()
		return err
	}

	if _, err := u.tx.Exec("INSERT OR REPLACE INTO Info(key,value,updated) VALUES (?,?,?)",
		"lastUpdate", "singleUpdate", time.Now().UTC().Unix(),
	); err != nil {
		return err
	}

	return u.tx.Commit()
}

func (u *UpdateMany) Update(ctx context.Context) error {
	var err error
	u.tx, err = u.Db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}

	hasUpdates, err := u.documents()
	if !hasUpdates || err != nil {
		slog.Debug("Error updating documents")
		u.tx.Rollback()
		return err
	}

	if err := u.tags(); err != nil {
		slog.Debug("Error updating tags")
		u.tx.Rollback()
		return err
	}

	if err := u.links(); err != nil {
		slog.Debug("Error updating links")
		u.tx.Rollback()
		return err
	}

	if err := u.authors(); err != nil {
		slog.Debug("Error updating authors")
		u.tx.Rollback()
		return err
	}

	if _, err := u.tx.Exec("INSERT OR REPLACE INTO Info(key,value,updated) VALUES (?,?,?)",
		"lastUpdate", "multiUpdate", time.Now().UTC().Unix(),
	); err != nil {
		return err
	}

	return u.tx.Commit()
}

// set document fields in db, returns if an update has occured
func (u *Update) document() (bool, error) {
	filetime := sql.NullInt64{Int64: u.Doc.FileTime.Unix(), Valid: !u.Doc.FileTime.IsZero()}
	row := u.tx.QueryRow(`
	SELECT TRUE
	FROM Documents
	WHERE path = ? AND COALESCE(fileTime,0) < COALESCE(?,0)
	`, u.Doc.Path, filetime)

	isUpdate := false
	if err := row.Scan(&isUpdate); err == sql.ErrNoRows {
		isUpdate = true
	} else if err != nil {
		return false, err
	}

	if !isUpdate {
		return false, nil
	}

	title := sql.NullString{String: u.Doc.Title, Valid: u.Doc.Title != ""}
	date := sql.NullInt64{Int64: u.Doc.Date.Unix(), Valid: !u.Doc.Date.IsZero()}
	meta := sql.NullString{String: u.Doc.OtherMeta, Valid: u.Doc.OtherMeta != ""}

	_, err := u.tx.Exec(`
	INSERT INTO Documents(path, title, date, fileTime, meta)
	VALUES (?,?,?,?,?)
	ON CONFLICT(path)
	DO UPDATE SET
		title=excluded.title,
		date=excluded.date,
		fileTime=excluded.fileTime,
		meta=excluded.meta
	`, u.Doc.Path, title, date, filetime, meta)
	if err != nil {
		return true, err
	}

	row = u.tx.QueryRow(`SELECT id FROM Documents WHERE path = ?`, u.Doc.Path)
	if err := row.Scan(&u.Id); err != nil {
		return true, err
	}

	return true, nil
}

func (u *UpdateMany) documents() (bool, error) {
	_, err := u.tx.Exec(`
	CREATE TEMPORARY TABLE updateDocs (
		path TEXT UNIQUE NOT NULL,
		title TEXT,
		date INT,
		fileTime INT,
		meta BLOB
	)`)
	if err != nil {
		return false, err
	}
	defer u.tx.Exec("DROP TABLE temp.updateDocs")

	tempInsertStmt, err := u.tx.Prepare("INSERT INTO temp.updateDocs VALUES (?,?,?,?,?)")
	if err != nil {
		return false, err
	}
	defer tempInsertStmt.Close()

	for path, doc := range u.PathDocs {
		filetime := sql.NullInt64{
			Int64: doc.FileTime.Unix(),
			Valid: !doc.FileTime.IsZero(),
		}
		title := sql.NullString{
			String: doc.Title,
			Valid:  doc.Title != "",
		}
		date := sql.NullInt64{
			Int64: doc.Date.Unix(),
			Valid: !doc.Date.IsZero(),
		}
		meta := sql.NullString{
			String: doc.OtherMeta,
			Valid:  doc.OtherMeta != "",
		}
		if _, err := tempInsertStmt.Exec(path, title, date, filetime, meta); err != nil {
			return false, err
		}
	}

	_, err = u.tx.Exec(`
	DELETE FROM Documents
	WHERE Documents.path NOT IN (
		SELECT path FROM temp.updateDocs
	)`)
	if err != nil {
		return false, err
	}

	_, err = u.tx.Exec(`
	INSERT INTO Documents (path, title, date, fileTime, meta)
	SELECT * FROM updateDocs WHERE TRUE
	ON CONFLICT(path) DO UPDATE SET
		title=excluded.title,
		date=excluded.date,
		fileTime=excluded.fileTime,
		meta=excluded.meta
	WHERE excluded.fileTime > Documents.fileTime
	`)
	if err != nil {
		return false, err
	}

	updates, err := u.tx.Query(`
	SELECT id, Documents.path
	FROM updateDocs
	JOIN Documents ON updateDocs.path = Documents.path
	WHERE Documents.fileTime = updateDocs.fileTime
	`)
	if err != nil {
		return false, err
	}
	defer updates.Close()

	u.Docs = make(map[int64]*index.Document)
	var id int64
	var path string
	hasUpdate := false
	for updates.Next() {
		if err := updates.Scan(&id, &path); err != nil {
			return false, err
		}
		u.Docs[id] = u.PathDocs[path]
		hasUpdate = true
	}

	return hasUpdate, nil
}

func (u Update) tags() error {
	if _, err := u.tx.Exec(`
	DELETE FROM DocumentTags
	WHERE docId = ?
	`, u.Id); err != nil {
		return err
	}

	query, args := BatchQuery(
		"INSERT OR IGNORE INTO Tags (name) VALUES",
		"", "(?)", ",", "",
		len(u.Doc.Tags), u.Doc.Tags,
	)
	if _, err := u.tx.Exec(query, args...); err != nil {
		return err
	}

	preqQuery := fmt.Sprintf(`
	INSERT INTO DocumentTags
		SELECT %d, Tags.id
		FROM Tags
		WHERE name in
	`, u.Id)
	query, args = BatchQuery(
		preqQuery, "(", "?", ",", ")",
		len(u.Doc.Tags), u.Doc.Tags,
	)

	if _, err := u.tx.Exec(query, args...); err != nil {
		return err
	}

	return nil
}

func (u UpdateMany) tags() error {
	// PERF: consider batching
	deleteStmt, err := u.tx.Prepare("DELETE FROM DocumentTags WHERE docId = ?")
	if err != nil {
		return err
	}
	defer deleteStmt.Close()

	for id := range u.Docs {
		if _, err := deleteStmt.Exec(id); err != nil {
			return err
		}
	}

	for id, doc := range u.Docs {
		if len(doc.Tags) == 0 {
			continue
		}
		insertTag, args := BatchQuery(
			"INSERT OR IGNORE INTO Tags (name) VALUES",
			"", "(?)", ",", "",
			len(doc.Tags), doc.Tags,
		)
		_, err = u.tx.Exec(insertTag, args...)
		if err != nil {
			return err
		}

		preqQuery := fmt.Sprintf(`
		INSERT INTO DocumentTags
			SELECT %d, Tags.id
			FROM Tags
			WHERE name in
		`, id)
		setDocTags, _ := BatchQuery(
			preqQuery, "(", "?", ",", ")",
			len(doc.Tags), doc.Tags,
		)
		if _, err := u.tx.Exec(setDocTags, args...); err != nil {
			return err
		}
	}

	return nil
}

func (u Update) links() error {
	if _, err := u.tx.Exec(`
	DELETE FROM Links
	WHERE docId = ?
	`, u.Id); err != nil {
		return err
	}

	query, args := BatchQuery(
		"INSERT INTO Links VALUES ",
		"", fmt.Sprintf("(%d,?)", u.Id), ",", "",
		len(u.Doc.Links), u.Doc.Links,
	)
	if _, err := u.tx.Exec(query, args...); err != nil {
		return err
	}

	return nil
}

func (u UpdateMany) links() error {
	deleteStmt, err := u.tx.Prepare("DELETE FROM Links WHERE docId = ?")
	if err != nil {
		return err
	}
	defer deleteStmt.Close()
	insertStmt, err := u.tx.Prepare("INSERT OR IGNORE INTO Links VALUES (?,?)")
	if err != nil {
		return err
	}
	defer insertStmt.Close()

	for id, doc := range u.Docs {
		if _, err := deleteStmt.Exec(id); err != nil {
			return err
		}

		for _, link := range doc.Links {
			if _, err := insertStmt.Exec(id, link); err != nil {
				return err
			}
		}
	}

	return nil
}

func (u Update) authors() error {
	if _, err := u.tx.Exec(`
	DELETE FROM DocumentAuthors
	WHERE docId = ?
	`, u.Id); err != nil {
		return err
	}

	tempTable, args := BatchQuery(`
		CREATE TEMPORARY TABLE new_names AS
		SELECT column1 AS name
		FROM ( VALUES `,
		"", "(?)", ",", ")",
		len(u.Doc.Authors), u.Doc.Authors,
	)
	_, err := u.tx.Exec(tempTable, args...)
	if err != nil {
		return err
	}
	defer u.tx.Exec("DROP TABLE temp.new_names")

	_, err = u.tx.Exec(`
	INSERT OR IGNORE INTO Authors(name)
	SELECT * FROM new_names
	`)
	if err != nil {
		return err
	}
	_, err = u.tx.Exec(`
	INSERT OR IGNORE INTO Aliases(alias)
	SELECT * FROM new_names
	`)
	if err != nil {
		return err
	}

	docAuthQuery := fmt.Sprintf(`
	INSERT INTO DocumentAuthors
	SELECT %d, existing.id
	FROM new_names
	LEFT JOIN (
		SELECT * FROM Authors
		UNION ALL
		SELECT * FROM Aliases
	) AS existing ON existing.name = new_names.name
	`, u.Id)
	if _, err := u.tx.Exec(docAuthQuery); err != nil {
		return err
	}

	return nil
}

func (u UpdateMany) authors() error {
	deleteStmt, err := u.tx.Prepare("DELETE FROM DocumentAuthors WHERE docId = ?")
	if err != nil {
		return err
	}
	defer deleteStmt.Close()

	_, err = u.tx.Exec(`
	CREATE TEMPORARY TABLE new_names (
		docId INTEGER NOT NULL,
		name TEXT NOT NULL,
		UNIQUE(docId, name)
	)`)
	if err != nil {
		return err
	}
	defer u.tx.Exec("DROP TABLE temp.new_names")

	insertTempTable, err := u.tx.Prepare("INSERT INTO temp.new_names VALUES (?,?)")
	if err != nil {
		return err
	}
	defer insertTempTable.Close()

	for id, doc := range u.Docs {
		if _, err := deleteStmt.Exec(id); err != nil {
			return err
		}

		for _, author := range doc.Authors {
			if _, err := insertTempTable.Exec(id, author); err != nil {
				return err
			}
		}
	}

	_, err = u.tx.Exec(`
	INSERT OR IGNORE INTO Authors(name)
	SELECT name FROM new_names
	`)
	if err != nil {
		return err
	}

	_, err = u.tx.Exec(`
	INSERT OR IGNORE INTO Aliases(alias)
	SELECT name FROM new_names
	`)
	if err != nil {
		return err
	}

	_, err = u.tx.Exec(`
	INSERT INTO DocumentAuthors
	SELECT docId, existing.id
	FROM new_names
	LEFT JOIN (
	SELECT * FROM Authors
	UNION ALL
	SELECT * FROM Aliases
	) AS existing ON existing.name = new_names.name
	`)

	return err
}
