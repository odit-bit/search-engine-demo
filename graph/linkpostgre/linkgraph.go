package linkpostgre

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jmoiron/sqlx"
	"github.com/odit-bit/linkstore/linkgraph"
)

var _ linkgraph.Graph = (*postgre)(nil)

type postgre struct {
	db *sqlx.DB
}

func New(db *sqlx.DB) *postgre {
	p := postgre{
		db: db,
	}
	if err := p.Migrate(); err != nil {
		log.Fatal(err)
	}
	return &p
}

// ==============

// const dropLinksTable = `
// DROP TABLE IF EXISTS links;
// `

// const dropEdgeTable = `
// DROP TABLE IF EXISTS edges;
// `

func (p *postgre) Migrate() error {
	//link table
	_, err := p.db.ExecContext(context.TODO(), createLinkTableQuery)
	if err != nil {
		return fmt.Errorf("create table: %v", err)
	}

	//edge table
	_, err = p.db.ExecContext(context.TODO(), createEdgeTableQuery)
	if err != nil {
		return fmt.Errorf("create table: %v", err)
	}

	return nil
}

// LookupLink implements graph.Graph.
func (p *postgre) LookupLink(id uuid.UUID) (*linkgraph.Link, error) {
	// queryCtx, cancel := context.WithCancel(p.ctx)
	// defer cancel()

	var link linkgraph.Link

	err := p.db.QueryRowx(lookupLinkQuery, id).Scan(&link.ID, &link.URL, &link.RetrievedAt)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, linkgraph.ErrNotFound
		}
		return nil, fmt.Errorf("lookup link: %v", err)
	}

	return &link, nil
}

// RemoveStaleEdges implements graph.Graph.
func (p *postgre) RemoveStaleEdges(fromID uuid.UUID, updatedBefore time.Time) error {
	// queryCtx, cancel := context.WithCancel(p.ctx)
	// defer cancel()

	_, err := p.db.Exec(edgeRemoveStaleQuery, fromID, updatedBefore.UTC())
	if err != nil {
		return fmt.Errorf("remove stale edge: %v", err)
	}

	return nil
}

// UpsertLink implements graph.Graph.
// TODO: make fix time standar so no need to call UTC() every time
func (p *postgre) UpsertLink(link *linkgraph.Link) error {
	// queryCtx, cancel := context.WithCancel(p.ctx)
	// defer cancel()

	link.RetrievedAt = link.RetrievedAt.UTC()
	err := p.db.QueryRowx(linkUpsertQuery, link.URL, link.RetrievedAt).Scan(
		&link.ID,
		&link.RetrievedAt,
	)
	if err != nil {
		return fmt.Errorf("upsert link: %v ", err)
	}

	return nil
}

// UpsertEdge implements graph.Graph.
// TODO: make fix time standar so no need to call UTC() every time
func (p *postgre) UpsertEdge(edge *linkgraph.Edge) error {
	// queryCtx, cancel := context.WithCancel(p.ctx)
	// defer cancel()

	edge.UpdateAt = edge.UpdateAt.UTC()

	err := p.db.QueryRowx(edgeUpsertQuery, edge.Src, edge.Dst).Scan(&edge.ID, &edge.UpdateAt)
	if err != nil {
		pgErr, ok := err.(*pgconn.PgError)
		if ok {
			switch pgErr.Code {
			case "23503":
				return linkgraph.ErrUnknownEdgeLinks
			}
		}

		return fmt.Errorf("edge upsert: %v", err)

	}
	return nil
}

// Links implements graph.Graph.
func (p *postgre) Links(fromID uuid.UUID, toID uuid.UUID, accessBefore time.Time) (linkgraph.LinkIterator, error) {
	// queryCtx, cancel := context.WithCancel(p.ctx)

	// rows := p.db.QueryRowxContext(context.Background(), linksQuery, fromID, toID, retrieveBefore.UTC())
	rows, err := p.db.Queryx(linksIterationQuery, fromID, toID, accessBefore.UTC())
	if err != nil {
		return nil, err
	}

	linkIterator := linkIterator{
		rows:    rows,
		lastErr: nil,
	}

	return &linkIterator, nil
}

//==========

// Edges implements graph.Graph.
func (p *postgre) Edges(fromID uuid.UUID, toID uuid.UUID, updateBefore time.Time) (linkgraph.EdgeIterator, error) {
	// queryCtx, cancel := context.WithCancel(p.ctx)

	//find edges row
	rows, err := p.db.Queryx(edgesIterationQuery, fromID, toID, updateBefore.UTC())
	if err != nil {
		return nil, fmt.Errorf("edge iterator: %v", err)
	}

	edgeIterator := edgeIterator{
		rows:    rows,
		lastErr: err,
	}

	return &edgeIterator, nil
}

//==========

var _ linkgraph.LinkIterator = (*linkIterator)(nil)

// linkedge iterator
type linkIterator struct {
	rows *sqlx.Rows

	link *linkgraph.Link

	lastErr error
}

// Close implements graph.LinkIterator.
func (it *linkIterator) Close() error {
	return it.rows.Close()
}

// Error implements graph.LinkIterator.
func (it *linkIterator) Error() error {
	return it.lastErr
}

// Link implements graph.LinkIterator.
func (it *linkIterator) Link() *linkgraph.Link {
	return it.link
}

// Next implements graph.LinkIterator.
func (it *linkIterator) Next() bool {

	ok := it.rows.Next()
	if !ok {
		return false
	}

	var link linkgraph.Link
	it.lastErr = it.rows.Scan(&link.ID, &link.URL, &link.RetrievedAt) //Scan(&link)
	if it.lastErr != nil {
		return false
	}

	it.link = &link
	return true

}

var _ linkgraph.EdgeIterator = (*edgeIterator)(nil)

type edgeIterator struct {
	rows *sqlx.Rows

	edge *linkgraph.Edge

	lastErr error
	// cancelFn context.CancelFunc
}

// Close implements linkgraph.EdgeIterator.
func (it *edgeIterator) Close() error {
	return it.rows.Close()
}

// Edge implements linkgraph.EdgeIterator.
func (it *edgeIterator) Edge() *linkgraph.Edge {
	return it.edge
}

// Error implements linkgraph.EdgeIterator.
func (it *edgeIterator) Error() error {
	return it.lastErr
}

// Next implements linkgraph.EdgeIterator.
func (it *edgeIterator) Next() bool {
	ok := it.rows.Next()
	if !ok {
		return false
	}

	var edge linkgraph.Edge
	it.lastErr = it.rows.Scan(&edge.ID, &edge.Src, &edge.Dst, &edge.UpdateAt)
	if it.lastErr != nil {
		return false
	}

	it.edge = &edge
	return true
}
