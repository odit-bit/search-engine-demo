package indexpostgre

import (
	"context"
	"fmt"
)

var tsvector = "to_tsvector('english', coalesce(title, '') || ' ' || coalesce(content,''))"


const dropDocumentsTable = `
	DROP TABLE IF EXISTS documents;
`

const dropDocumentsIndex = `
	DROP INDEX ts_idx;
`

func (idx *indexer) drop() error {
	_, err := idx.db.ExecContext(context.TODO(), dropDocumentsIndex)
	if err != nil {
		return err
	}

	_, err = idx.db.ExecContext(context.TODO(), dropDocumentsTable)
	if err != nil {
		return err
	}
	return nil
}

// create table
const createDocumentsTable = `
	CREATE TABLE IF NOT EXISTS documents(
		linkID uuid UNIQUE NOT NULL,
		url text NOT NULL,
		title text,
		content text,
		indexed_at TIMESTAMP NOT NULL DEFAULT NOW(),
		pagerank double precision
	);
`

// full text search implementation

var (
	//generate text-search column for table
	alterColumnSearch = fmt.Sprintf(`
	ALTER TABLE documents
	ADD COLUMN IF NOT EXISTS ts tsvector GENERATED ALWAYS AS (%s) STORED;
`, tsvector)

	//create index for text-search
	createSearchIndex = fmt.Sprintf(`
	CREATE INDEX IF NOT EXISTS ts_idx ON documents USING gin(%v)
`, tsvector)
)

func (idx *indexer) migrate() error {
	//create table
	_, err := idx.db.ExecContext(context.TODO(), createDocumentsTable)
	if err != nil {
		return fmt.Errorf("create table: %v", err)
	}

	// alter columns ts
	_, err = idx.db.ExecContext(context.TODO(), alterColumnSearch)
	if err != nil {
		return fmt.Errorf("alter ts column: %v", err)
	}

	//create index search
	_, err = idx.db.ExecContext(context.TODO(), createSearchIndex)
	if err != nil {
		return err
	}
	return nil
}
