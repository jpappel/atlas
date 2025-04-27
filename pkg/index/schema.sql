-- TABLE of config values
CREATE TABLE Indexes(
    root TEXT NOT NULL,
    followSym DATE
);

-- Schema
CREATE TABLE Documents(
    id INTEGER PRIMARY KEY,
    path TEXT UNIQUE NOT NULL,
    title TEXT,
    date INT,
    fileTime INT,
    meta BLOB
);

CREATE TABLE Authors(
    id INTEGER PRIMARY KEY,
    name TEXT UNIQUE NOT NULL
);

CREATE TABLE Aliases(
    authorId INT NOT NULL,
    alias TEXT UNIQUE NOT NULL,
    FOREIGN KEY (authorId) REFERENCES Authors(id)
);

CREATE TABLE Tags(
    id INTEGER PRIMARY KEY,
    name TEXT UNIQUE NOT NULL,
);

CREATE TABLE Links(
    referencedId INT,
    refererId INT,
    FOREIGN KEY (referencedId) REFERENCES Documents(id),
    FOREIGN KEY (refererId) REFERENCES Documents(id)
);

CREATE TABLE DocumentAuthors(
    docId INT NOT NULL,
    authorId INT NOT NULL,
    FOREIGN KEY (docId) REFERENCES Documents(id),
    FOREIGN KEY (authorId) REFERENCES Authors(id)
);

CREATE TABLE DocumentTags(
    docId INT NOT NULL,
    tagId INT NOT NULL,
    FOREIGN KEY (docId) REFERENCES Documents(id),
    FOREIGN KEY (tagId) REFERENCES Tags(id),
    UNIQUE(docId, tagId)
);

-- Indexes
CREATE INDEX idx_doc_dates
ON Documents (date);
CREATE INDEX idx_doc_titles
ON Documents (title);

CREATE INDEX idx_author_name
ON Authors(name);

CREATE INDEX idx_aliases_alias
ON Aliases(alias);
CREATE INDEX idx_aliases_authorId
ON Aliases(authorId);

CREATE INDEX idx_doctags_tagid
ON DocumentTags (tagId);
