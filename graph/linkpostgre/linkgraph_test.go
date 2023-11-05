package linkpostgre

import (
	"context"
	"fmt"
	"log"
	"math/big"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
	"github.com/odit-bit/linkstore/linkgraph"
)

type Migrate struct {
	Create string
	Drop   string
}

var linkTable = Migrate{
	Create: `
		CREATE TABLE IF NOT EXISTS links(
			id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
			url text UNIQUE,
			retrieved_at TIMESTAMP
		);
	`,
	Drop: `
		DROP TABLE IF EXISTS links;
	`,
}

var edgeTable = Migrate{
	Create: `
		CREATE TABLE IF NOT EXISTS edges(
			id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
			src UUID NOT NULL REFERENCES links(id) ON DELETE CASCADE,
			dst UUID NOT NULL REFERENCES links(id) ON DELETE CASCADE,
			update_at TIMESTAMP,
			CONSTRAINT edge_links UNIQUE(src,dst)
		);
	`,
	Drop: `
		DROP TABLE IF EXISTS edges;
	`,
}

var pg = func() *postgre {
	conn, err := sqlx.Connect("pgx", "host=localhost user=development password=credential dbname=development sslmode=disable")
	if err != nil {
		log.Fatalf("connect errror: %v", err)
	}
	err = conn.Ping()
	if err != nil {
		log.Fatal(err)
	}

	pg := New(conn)
	return pg
}()

func Test_postgredb(t *testing.T) {
	t.Run("link upsert logic", test_upsert_link)
	t.Run("link lookup logic", test_lookup_link)
	t.Run("link iterator", test_concurrent_link_iterators)

	t.Run("link iterator filter login logic", test_Link_iterator_Timefilter)

	t.Run("edge upsert logic", test_upsert_edge)

}

func test_upsert_edge(t *testing.T) {
	pg.db.ExecContext(context.TODO(), linkTable.Create)
	pg.db.ExecContext(context.TODO(), edgeTable.Create)
	defer func() {
		pg.db.ExecContext(context.TODO(), edgeTable.Drop)
		pg.db.ExecContext(context.TODO(), linkTable.Drop)
	}()

	// Create links
	linkUUIDs := make([]uuid.UUID, 3)
	for i := 0; i < 3; i++ {
		link := &linkgraph.Link{URL: fmt.Sprint(i)}
		err := pg.UpsertLink(link)
		if err != nil {
			t.Fatal(err)
		}
		linkUUIDs[i] = link.ID
	}

	original := linkgraph.Edge{
		Src: linkUUIDs[0],
		Dst: linkUUIDs[1],
	}

	err := pg.UpsertEdge(&original)
	if err != nil {
		t.Fatal(err)
	}

	if original.ID.String() == uuid.Nil.String() {
		t.Fatalf("\ngot:%v \nmessage:%v", original.ID.String(),
			"orginal edge id not set")
	}
	if original.UpdateAt.Unix() == 0 {
		t.Fatalf("\ngot:%v \nmessage:%v", original.UpdateAt.String(),
			"orginal updateAt field not set")
	}

	// Update existing edge
	other := &linkgraph.Edge{
		ID:  original.ID,
		Src: linkUUIDs[0],
		Dst: linkUUIDs[1],
	}

	err = pg.UpsertEdge(other)
	if err != nil {
		t.Fatal(err)
	}

	if other.ID.String() != original.ID.String() {
		t.Fatalf("\ngot:%v\nExpect:%v\n:message:%v", other.ID.String(), original.ID.String(),
			"orginal edge id change while updating (upsert)")
	}

	if other.UpdateAt.String() == original.UpdateAt.String() {
		t.Fatalf("\ngot:%v\nExpect:%v\nmessage:%v", other.UpdateAt.String(), original.UpdateAt.String(),
			"orginal edge UpdateAt field not modified")
	}

	// create edge with unknown id link
	unkwn := &linkgraph.Edge{
		ID:  uuid.New(),
		Src: linkUUIDs[0],
		Dst: uuid.New(),
	}

	err = pg.UpsertEdge(unkwn)
	if err != nil {
		if err != linkgraph.ErrUnknownEdgeLinks {
			t.Fatalf("\ngot: %v\nexpect: %v", err, linkgraph.ErrUnknownEdgeLinks)
		}
	}
}

func test_upsert_link(t *testing.T) {
	pg.db.ExecContext(context.TODO(), linkTable.Create)
	pg.db.ExecContext(context.TODO(), edgeTable.Create)
	defer func() {
		pg.db.ExecContext(context.TODO(), edgeTable.Drop)
		pg.db.ExecContext(context.TODO(), linkTable.Drop)
	}()

	//=======================
	// Create a new link
	original := &linkgraph.Link{
		URL:         "https://example.com",
		RetrievedAt: time.Now().Add(-10 * time.Hour),
	}
	err := pg.UpsertLink(original)
	if err != nil {
		t.Fatal(err)
	}

	//=============================
	// Update existing link with a newer timestamp and different URL
	accessedAt := time.Now().Truncate(time.Second).UTC()
	existing := &linkgraph.Link{
		ID:          original.ID,
		URL:         "https://example.com",
		RetrievedAt: accessedAt,
	}
	err = pg.UpsertLink(existing)
	if err != nil {
		t.Fatal(err)
	}
	if existing.ID != original.ID {
		t.Errorf("\ngot:\t %v, \nexpected:\t %v \nerror: %v", existing.ID, original.ID,
			"value of ID should same")
	}

	stored, err := pg.LookupLink(existing.ID)
	if err != nil {
		t.Fatal(err)
	}
	if stored.RetrievedAt != accessedAt {
		t.Errorf("\ngot:\t %v, \nexpected:\t %v \nerror: %v", stored.RetrievedAt, accessedAt,
			"last accessed timestamp was not updated")
	}

	//=============================
	// Attempt to insert a new link whose URL matches an existing link with
	// and provide an older accessedAt value
	sameURL := &linkgraph.Link{
		URL:         existing.URL,
		RetrievedAt: time.Now().Add(-10 * time.Hour).UTC(),
	}
	err = pg.UpsertLink(sameURL)
	if err != nil {
		t.Fatal(err)
	}
	if existing.ID != sameURL.ID {
		t.Errorf("\ngot:\t %v, \nexpected:\t %v \nerror: %v", existing.ID, sameURL.ID,
			"value of ID should same")
	}

	stored, err = pg.LookupLink(existing.ID)
	if err != nil {
		t.Fatal(err)
	}

	if stored.RetrievedAt != accessedAt {
		t.Errorf("\ngot:\t %v, \nexpected:\t %v \nerror: %v", stored.RetrievedAt, accessedAt,
			"last accessed timestamp was overwritten with an older value")
	}

	//=============================
	// Create a new link and then attempt to update its URL to the same as
	// an existing link.
	dup := &linkgraph.Link{
		URL: "foo",
	}
	err = pg.UpsertLink(dup)
	if err != nil {
		t.Fatal(err)
	}

	// c.Assert(dup.ID, gc.Not(gc.Equals), uuid.Nil, gc.Commentf("expected a linkID to be assigned to the new link"))
	if dup.ID == uuid.Nil {
		t.Errorf("\ngot:\t %v, \nexpected:\t %v \nerror: %v", dup.ID, uuid.Nil,
			"last accessed timestamp was overwritten with an older value")
	}

	//===============================
	//l

}

func test_lookup_link(t *testing.T) {
	pg.db.ExecContext(context.TODO(), linkTable.Create)
	pg.db.ExecContext(context.TODO(), edgeTable.Create)
	defer func() {
		pg.db.ExecContext(context.TODO(), edgeTable.Drop)
		pg.db.ExecContext(context.TODO(), linkTable.Drop)
	}()

	//====================================
	// Create a new link
	link := &linkgraph.Link{
		URL:         "https://example.com",
		RetrievedAt: time.Now().Truncate(time.Second).UTC(),
	}

	err := pg.UpsertLink(link)
	if err != nil {
		t.Fatal(err)
	}
	if link.ID == uuid.Nil {
		t.Fatalf("\ngot:\t %v, \nexpected:\t %v \nerror: %v", link.ID, "not nill",
			"expected a linkID to be assigned to the new link")
	}

	// Lookup link by ID
	other, err := pg.LookupLink(link.ID)
	if err != nil {
		t.Fatal(err)
	}
	if other.ID.String() != link.ID.String() {
		t.Fatalf("\ngot:\t %v, \nexpected:\t %v \nerror: %v", other.ID, link.ID,
			"lookup by ID returned the wrong link")
	}

	// Lookup link by unknown ID
	_, err = pg.LookupLink(uuid.Nil)
	if err != nil {
		if err != linkgraph.ErrNotFound {
			t.Fatalf("error should %v, got: %v", linkgraph.ErrNotFound, err)
		}
	}

}

func test_concurrent_link_iterators(t *testing.T) {
	pg.db.ExecContext(context.TODO(), linkTable.Create)
	pg.db.ExecContext(context.TODO(), edgeTable.Create)
	defer func() {
		pg.db.ExecContext(context.TODO(), edgeTable.Drop)
		pg.db.ExecContext(context.TODO(), linkTable.Drop)
	}()

	// testing
	var (
		wg           sync.WaitGroup
		numIterators = 10
		numLinks     = 100
	)

	for i := 0; i < numLinks; i++ {
		l := linkgraph.Link{URL: fmt.Sprint(i)}
		err := pg.UpsertLink(&l)
		if err != nil {
			t.Fatal(err)
		}
	}

	errC := make(chan error)
	wg.Add(numIterators)
	for i := 0; i < numIterators; i++ {

		go func(id int) {
			defer wg.Done()
			iterTagComment := fmt.Sprintf("iterator %d", id)
			seen := make(map[string]bool)
			iter, err := partitionLinkIter(pg, t, 0, 1, time.Now())
			if err != nil {
				errC <- fmt.Errorf("error: %v,  at %v", err, iterTagComment)
				return
			}

			defer func() {
				err := iter.Close()
				if err != nil {
					errC <- err
					return
				}

			}()

			for i := 0; iter.Next(); i++ {
				l := iter.Link()
				linkID := l.ID.String()
				if seen[linkID] {
					errC <- fmt.Errorf("iterator %d saw same link twice", id)
					return
				}
				seen[linkID] = true
			}

			if len(seen) != numLinks {
				errC <- fmt.Errorf("got:%v, expected: %v,  at:%v", len(seen), numLinks, iterTagComment)
				return
			}
			if iter.Error() != nil {
				errC <- fmt.Errorf("err happen at iterator: %v", iter.Error())
				return
			}
			if iter.Close() != nil {
				errC <- fmt.Errorf("err closing iterator: %v", iter.Close())
				return
			}

		}(i)

	}
	doneCh := make(chan struct{})
	go func() {
		wg.Wait()
		close(doneCh)
	}()

	select {
	case <-doneCh:
	// test completed successfully
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for test to complete")
	case err := <-errC:
		t.Fatal(err)
	}
}

func test_Link_iterator_Timefilter(t *testing.T) {
	pg.db.ExecContext(context.TODO(), linkTable.Create)
	pg.db.ExecContext(context.TODO(), edgeTable.Create)
	defer func() {
		pg.db.ExecContext(context.TODO(), edgeTable.Drop)
		pg.db.ExecContext(context.TODO(), linkTable.Drop)
	}()

	//=======================
	linkUUID := make([]uuid.UUID, 3)
	linkInsertTime := make([]time.Time, len(linkUUID))

	for i := 0; i < len(linkUUID); i++ {
		link := &linkgraph.Link{URL: fmt.Sprint(i), RetrievedAt: time.Now()}
		err := pg.UpsertLink(link)
		if err != nil {
			t.Fatal(err)
		}
		linkUUID[i] = link.ID
		linkInsertTime[i] = time.Now()
	}

	for i, ti := range linkInsertTime {
		var result []uuid.UUID
		linkCursor, err := partitionLinkIter(pg, t, 0, 1, ti)
		if err != nil {
			t.Fatal(err)
		}

		expected := linkUUID[:i+1]
		for linkCursor.Next() {

			act := linkCursor.Link()
			result = append(result, act.ID)
		}

		linkCursor.Close()

		sort.Slice(expected, func(l, r int) bool { return expected[l].String() < expected[r].String() })
		sort.Slice(result, func(l, r int) bool { return result[l].String() < result[r].String() })
		for i, res := range result {
			if res.String() != expected[i].String() {
				t.Fatalf("\n%v\n%v\n", res.String(), expected[i].String())
			}
		}
	}
}

func partitionLinkIter(pg linkgraph.Graph, t *testing.T, partition, numPartition int, accessBefore time.Time) (linkgraph.LinkIterator, error) {
	from, to := partitionRange(t, partition, numPartition)
	return pg.Links(from, to, accessBefore)
}

func partitionRange(t *testing.T, partition, numPartition int) (from, to uuid.UUID) {
	if partition < 0 || partition >= numPartition {
		t.Fatal("invalid partition")
	}

	var minUUID = uuid.Nil
	var maxUUID = uuid.MustParse("ffffffff-ffff-ffff-ffff-ffffffffffff")
	var err error

	//calculate size of each partition as(2^128/numPartition)
	tokenRange := big.NewInt(0)
	partSize := big.NewInt(0)
	partSize.SetBytes(maxUUID[:])
	partSize = partSize.Div(partSize, big.NewInt(int64(numPartition)))

	if partition == 0 {
		from = minUUID
	} else {
		tokenRange.Mul(partSize, big.NewInt(int64(partition)))
		from, err = uuid.FromBytes(tokenRange.Bytes())
		if err != nil {
			t.Fatal(err)
		}
	}

	if partition == numPartition-1 {
		to = maxUUID
	} else {
		tokenRange.Mul(partSize, big.NewInt(int64(partition+1)))
		to, err = uuid.FromBytes(tokenRange.Bytes())
		if err != nil {
			t.Fatal(err)
		}
	}

	return from, to

}
