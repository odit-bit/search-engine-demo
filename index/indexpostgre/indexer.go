package indexpostgre

import (
	"context"
	"fmt"

	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
	"github.com/odit-bit/indexstore/index"
)

// it is like page-size
var batchSize int = 10

var _ index.Indexer = (*indexer)(nil)

type indexer struct {
	db *sqlx.DB
}

func New(db *sqlx.DB) (*indexer, error) {
	idx := indexer{
		db: db,
	}

	err := idx.migrate()
	if err != nil {
		return nil, fmt.Errorf("postgreindex migrate: %v", err)
	}

	return &idx, nil

}

// Index implements index.Indexer.
// it uses to insert new document
func (i *indexer) Index(doc *index.Document) error {

	if doc.LinkID == uuid.Nil {
		return fmt.Errorf("indexer insert document: uuid cannot be nil")
	}
	doc.IndexedAt = doc.IndexedAt.UTC()
	_, err := i.db.ExecContext(context.TODO(), insertDocumentQuery, doc.LinkID, doc.URL, doc.Title, doc.Content, doc.IndexedAt, doc.Pagerank)
	if err != nil {
		return fmt.Errorf("indexer insert document error: %v, doc detail: %v", err, doc.URL)
	}
	return nil
}

// Find implements index.Indexer.
func (idx *indexer) Find(linkID uuid.UUID) (*index.Document, error) {
	var doc index.Document
	err := idx.db.QueryRowxContext(context.TODO(), findDocumentQuery, linkID).Scan(
		&doc.LinkID,
		&doc.URL,
		&doc.Title,
		&doc.Content,
		&doc.IndexedAt,
		&doc.Pagerank,
	)
	if err != nil {
		return nil, fmt.Errorf("indexer lookup document: %v", err)
	}
	return &doc, nil
}

// Search implements index.Indexer.
func (idx *indexer) Search(query index.Query) (index.Iterator, error) {

	var queryDoc, queryCount string
	var matchedCount int
	var rows *sqlx.Rows
	var err error

	pageSize := batchSize
	offset := query.Offset

	if query.Expression == "" {
		queryDoc = searchAllQuery
		queryCount = searchAllCountQuery

		//get the matchedCount document
		err := idx.db.QueryRowxContext(context.TODO(), queryCount).Scan(&matchedCount)
		if err != nil {
			return nil, fmt.Errorf("index search documents matched count: %v", err)
		}

		rows, err = idx.db.QueryxContext(context.TODO(), queryDoc, offset, pageSize)
		if err != nil {
			return nil, fmt.Errorf("index search documents: %v", err)
		}

	} else {
		switch query.Type {
		case 1:
			queryDoc = searchPhraseQuery
			queryCount = searchPhraseCountQuery
		default:
			queryDoc = searchMatchQuery
			queryCount = searchMatchCountQuery
		}

		//get the matchedCount document
		err := idx.db.QueryRowxContext(context.TODO(), queryCount, query.Expression).Scan(&matchedCount)
		if err != nil {
			return nil, fmt.Errorf("index search documents matched count: %v", err)
		}

		rows, err = idx.db.QueryxContext(context.TODO(), queryDoc, query.Expression, offset, pageSize)
		if err != nil {
			return nil, fmt.Errorf("index search documents: %v", err)
		}
	}

	docIterator := iterator{
		rows:         rows,
		latchedDoc:   nil,
		latchedErr:   nil,
		expression:   query.Expression,
		totalMatched: matchedCount,
	}
	return &docIterator, err
}

// UpdateScore implements index.Indexer.
// update the pagerank score's dcoument by linkID
// UpdateRank implements index.Indexer.
func (idx *indexer) UpdateRank(linkID uuid.UUID, score float64) error {
	_, err := idx.db.ExecContext(context.TODO(), updateScoreQuery, score, linkID)
	if err != nil {
		return fmt.Errorf("update pagerank document : %v", err)
	}
	return nil
}

// ================= iterator

var _ index.Iterator = (*iterator)(nil)

type iterator struct {
	rows *sqlx.Rows
	// fetch      *sqlx.Stmt
	latchedDoc *index.Document
	latchedErr error

	expression   string
	totalMatched int
}

// Close implements index.Iterator.
func (it *iterator) Close() error {
	err := it.rows.Close()
	if err != nil {
		return err
	}

	return nil

}

// Document implements index.Iterator.
func (it *iterator) Document() *index.Document {
	return it.latchedDoc
}

// Error implements index.Iterator.
func (it *iterator) Error() error {
	return it.latchedErr
}

// Next implements index.Iterator.
func (it *iterator) Next() bool {
	return it.fecthDoc()

}

// TotalCount implements index.Iterator.
func (it *iterator) TotalCount() uint64 {
	return uint64(it.totalMatched)
}

func (it *iterator) fecthDoc() bool {
	ok := it.rows.Next()

	if !ok {
		it.latchedErr = it.rows.Err()
		return false
	}

	var doc index.Document
	err := it.rows.Scan(
		&doc.LinkID,
		&doc.URL,
		&doc.Title,
		&doc.Content,
		&doc.IndexedAt,
		&doc.Pagerank,
	)
	if err != nil {
		it.latchedErr = err
		return false
	}
	it.latchedDoc = &doc
	return true
}
