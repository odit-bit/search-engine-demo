package linkcrawler

import (
	"time"

	"github.com/google/uuid"
	"github.com/odit-bit/indexstore/index"
	"github.com/odit-bit/linkstore/linkgraph"
)

type DocIndexer interface {
	Index(doc *index.Document) error
}

type GraphUpdater interface {
	// linkgraph.Graph

	UpsertLink(link *linkgraph.Link) error

	UpsertEdge(edge *linkgraph.Edge) error

	//return link iterator to iterate link in graph
	Links(fromID, toID uuid.UUID, retrieveBefore time.Time) (linkgraph.LinkIterator, error)

	// RemoveStaleEdges removes any edge that originates from the specified
	// link ID and was updated before the specified timestamp.
	RemoveStaleEdges(fromID uuid.UUID, updatedBefore time.Time) error
}

type LinkFetcher interface {
	//return link iterator to iterate link in graph
	linkgraph.LinkIterator
}
