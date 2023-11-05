package linkcrawler

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/odit-bit/indexstore/index"
	"github.com/odit-bit/linkstore/linkgraph"
	"github.com/odit-bit/webcrawler"
	"github.com/odit-bit/webcrawler/x/xpipe"
)

var minUUID = uuid.Nil
var maxUUID = uuid.MustParse("ffffffff-ffff-ffff-ffff-ffffffffffff")
var default_interval = 1 * time.Minute
var default_recrawl_interval = 7 * 24 * time.Hour

type CrawlService struct {
	crawler  *webcrawler.Crawler
	graphAPI GraphUpdater
	indexAPI DocIndexer

	Interval        time.Duration //default 1 * time.Minute
	RecrawlInterval time.Duration
}

func New(graphAPI GraphUpdater, indexAPI DocIndexer) *CrawlService {
	s := CrawlService{
		crawler:  webcrawler.NewCrawler(),
		graphAPI: graphAPI,
		indexAPI: indexAPI,

		Interval:        default_interval,
		RecrawlInterval: default_recrawl_interval,
	}
	return &s
}

func (la *CrawlService) Run(ctx context.Context) error {
	ticker := time.NewTicker(la.Interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			err := la.startCrawl(ctx)
			if err != nil {
				return err
			}
			ticker.Reset(la.Interval)
		}
	}
}

//underlying crawler need fetcher and streamer for source and sink, that this type profided
//from api

func (la *CrawlService) startCrawl(ctx context.Context) error {

	producer, err := la.Fetcher(minUUID, maxUUID, time.Now().Add(-la.RecrawlInterval))
	if err != nil {
		return err
	}

	consumer, err := la.Consumer()
	if err != nil {
		return err
	}

	err = la.crawler.Crawl(ctx, producer, consumer)
	producer.Close()

	log.Println("fecthed link:", producer.counter)
	log.Println("dispatched link:", consumer.counter)

	return err
}

// return fetcher to supply data for pipe
func (li *CrawlService) Fetcher(fromID, toID uuid.UUID, retrieveBefore time.Time) (*linkFetcher, error) {
	iter, err := li.graphAPI.Links(fromID, toID, retrieveBefore)
	if err != nil {
		return nil, err
	}

	fetcher := &linkFetcher{
		LinkIterator: iter,
	}

	return fetcher, nil
}

// return streamer to send data from pipe
func (li *CrawlService) Consumer() (*linkConsumer, error) {
	consumer := &linkConsumer{
		GraphUpdater: li.graphAPI,
		DocIndexer:   li.indexAPI,
	}

	return consumer, nil
}

var _ xpipe.Fetcher[*webcrawler.Resource] = (*linkFetcher)(nil)

type linkFetcher struct {
	counter int
	linkgraph.LinkIterator
}

// Resource implements xpipe.Fetcher.
func (lf *linkFetcher) Resource() *webcrawler.Resource {
	l := lf.Link()
	resource := webcrawler.NewResource()
	resource.ID = l.ID
	resource.URL = l.URL

	lf.counter++
	return resource
}

var _ xpipe.Streamer[*webcrawler.Resource] = (*linkConsumer)(nil)

type linkConsumer struct {
	counter int
	GraphUpdater
	DocIndexer
}

// Consume implements xpipe.Streamer.
func (ld *linkConsumer) Consume(ctx context.Context, result <-chan *webcrawler.Resource) error {
	for {
		select {
		case <-ctx.Done():
			return nil
		case r, ok := <-result:
			if !ok {
				return nil
			}
			err := ld.upsertResource(r)
			if err != nil {
				return err
			}

		}
	}
}

func (ld *linkConsumer) upsertResource(r *webcrawler.Resource) error {
	defer r.Put() //bug potential

	var wg sync.WaitGroup
	var err error

	//upsert link
	n := len(r.FoundURLs)
	foundURls := make([]string, n)
	nn := copy(foundURls, r.FoundURLs)
	if nn != n {
		return fmt.Errorf("link consumer : failed copy slice")
	}

	link := &linkgraph.Link{
		ID:          r.ID,
		URL:         r.URL,
		RetrievedAt: time.Now(),
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		newErr := ld.upsertLinkEdge(link, foundURls)
		err = errors.Join(err, newErr)
	}()

	//index doc
	doc := &index.Document{
		LinkID:    r.ID,
		URL:       r.URL,
		Title:     string(r.Title),
		Content:   string(r.Content),
		IndexedAt: time.Now(),
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		newErr := ld.Index(doc)
		err = errors.Join(err, newErr)
	}()

	wg.Wait()
	return err
}

func (ld *linkConsumer) upsertLinkEdge(link *linkgraph.Link, foundURLs []string) error {

	if err := ld.UpsertLink(link); err != nil {
		return err
	}

	for _, dst := range foundURLs {
		dstLink := &linkgraph.Link{
			URL: dst,
		}

		//insert link destination as node
		err := ld.UpsertLink(dstLink)
		if err != nil {
			return err
		}
		ld.counter++

		//insert link destination as edge
		edge := linkgraph.Edge{

			Src: link.ID,
			Dst: dstLink.ID,
		}

		if err := ld.UpsertEdge(&edge); err != nil {
			return err
		}
		ld.counter++

		removeEdgeBefore := time.Now()
		if err := ld.RemoveStaleEdges(link.ID, removeEdgeBefore); err != nil {
			return err
		}

	}

	return nil
}
