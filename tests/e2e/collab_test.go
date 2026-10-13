package e2e

import (
	"context"
	"math/rand/v2"
	"slices"
	"sync"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"

	documentv1 "github.com/rowjay007/collab-sphere/gen/document/v1"
	"github.com/rowjay007/collab-sphere/pkg/collabclient"
	"github.com/rowjay007/collab-sphere/pkg/crdt"
)

const letters = "abcdefghijklmnopqrstuvwxyz "

func randomEdit(rng *rand.Rand, c *collabclient.Client) error {
	return c.Edit(func(r *crdt.Replica) ([]crdt.Op, error) {
		if r.Len() > 0 && rng.IntN(10) < 3 {
			op, err := r.DeleteAt(rng.IntN(r.Len()))
			return []crdt.Op{op}, err
		}
		op, err := r.InsertAt(rng.IntN(r.Len()+1), rune(letters[rng.IntN(len(letters))]))
		return []crdt.Op{op}, err
	})
}

// settle waits until every client's ops are echoed and every client has
// received every other client's ops, then asserts identical state.
func settle(t *testing.T, ctx context.Context, clients []*collabclient.Client) string {
	t.Helper()
	for _, c := range clients {
		if err := c.WaitAcked(ctx); err != nil {
			t.Fatalf("wait acked: %v (errors from hub: %v)", err, c.Errors())
		}
	}
	var last []crdt.ID
	for _, c := range clients {
		c.Read(func(r *crdt.Replica) {
			if clock := r.Vector()[r.Client()]; clock > 0 {
				last = append(last, crdt.ID{Clock: clock, Client: r.Client()})
			}
		})
	}
	for _, c := range clients {
		err := c.WaitUntil(ctx, func(r *crdt.Replica) bool {
			v := r.Vector()
			for _, id := range last {
				if !v.Covers(id) {
					return false
				}
			}
			return true
		})
		if err != nil {
			t.Fatalf("wait for peers' ops: %v", err)
		}
	}
	want := clients[0].Snapshot()
	text := clients[0].Text()
	for i, c := range clients {
		if errs := c.Errors(); len(errs) > 0 {
			t.Fatalf("client %d got hub errors: %v", i, errs)
		}
		if got := c.Text(); got != text {
			t.Fatalf("client %d text diverged:\n got %q\nwant %q", i, got, text)
		}
		if !proto.Equal(c.Snapshot(), want) {
			t.Fatalf("client %d snapshot diverged from client 0", i)
		}
	}
	return text
}

func loadText(t *testing.T, ctx context.Context, docs documentv1.DocumentServiceClient, docID string) (string, *documentv1.Document) {
	t.Helper()
	resp, err := docs.LoadState(ctx, &documentv1.LoadStateRequest{Id: docID})
	if err != nil {
		t.Fatal(err)
	}
	doc, err := crdt.FromSnapshot(resp.GetSnapshot())
	if err != nil {
		t.Fatal(err)
	}
	for _, p := range resp.GetOperations() {
		op, err := crdt.OpFromProto(p)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := doc.Apply(op); err != nil {
			t.Fatal(err)
		}
	}
	return doc.Text(), resp.GetDocument()
}

func TestTwoClientsConvergeAndSurviveRestarts(t *testing.T) {
	s := newStack(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	docID := s.createDocument(ctx)

	alice := s.dial(ctx, docID, 1001)
	bob := s.dial(ctx, docID, 1002)
	clients := []*collabclient.Client{alice, bob}

	var wg sync.WaitGroup
	start := make(chan struct{})
	for i, c := range clients {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			for range 50 {
				if err := c.InsertAt(0, rune('A'+i)); err != nil {
					t.Error(err)
					return
				}
			}
		}()
	}
	close(start)
	wg.Wait()
	sameSpot := settle(t, ctx, clients)
	if len([]rune(sameSpot)) != 100 {
		t.Fatalf("same-position typing lost characters: %q", sameSpot)
	}

	start = make(chan struct{})
	for i, c := range clients {
		wg.Add(1)
		go func() {
			defer wg.Done()
			rng := rand.New(rand.NewPCG(uint64(i), 42))
			<-start
			for range 300 {
				if err := randomEdit(rng, c); err != nil {
					t.Error(err)
					return
				}
			}
		}()
	}
	close(start)
	wg.Wait()
	final := settle(t, ctx, clients)
	t.Logf("converged on %d visible runes after 700 ops", len([]rune(final)))

	stored, meta := loadText(t, ctx, s.docs, docID)
	if stored != final {
		t.Fatalf("document-service state differs from clients:\n got %q\nwant %q", stored, final)
	}
	if meta.GetOpSeq() != 700 {
		t.Fatalf("op_seq = %d, want 700", meta.GetOpSeq())
	}

	alice.Close()
	bob.Close()
	s.hub.stop()
	s.docsvc.stop()
	s.startDocumentService()
	s.startHub()

	_, meta = loadText(t, ctx, s.docs, docID)
	if meta.GetOpSeq()-meta.GetSnapshotSeq() >= 100 || meta.GetSnapshotSeq() == 0 {
		t.Fatalf("snapshot_seq = %d with op_seq %d breaks the 100-op cadence", meta.GetSnapshotSeq(), meta.GetOpSeq())
	}

	carol := s.dial(ctx, docID, 1003)
	if got := carol.Text(); got != final {
		t.Fatalf("after restarting both services, DocumentSync text differs:\n got %q\nwant %q", got, final)
	}
	if err := carol.InsertAt(0, '!'); err != nil {
		t.Fatal(err)
	}
	if err := carol.WaitAcked(ctx); err != nil {
		t.Fatal(err)
	}
	if stored, _ := loadText(t, ctx, s.docs, docID); stored != "!"+final {
		t.Fatalf("edit after restart not persisted: %q", stored)
	}
}

func TestHundredConcurrentEditors(t *testing.T) {
	if testing.Short() {
		t.Skip("load test")
	}
	s := newStack(t)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	docID := s.createDocument(ctx)

	const editors, edits = 100, 40
	clients := make([]*collabclient.Client, editors)
	for i := range clients {
		clients[i] = s.dial(ctx, docID, crdt.ClientID(5000+i))
	}

	var wg sync.WaitGroup
	start := make(chan struct{})
	for i, c := range clients {
		wg.Add(1)
		go func() {
			defer wg.Done()
			rng := rand.New(rand.NewPCG(uint64(i), 7))
			<-start
			for range edits {
				time.Sleep(time.Duration(100+rng.IntN(100)) * time.Millisecond)
				if err := randomEdit(rng, c); err != nil {
					t.Error(err)
					return
				}
			}
		}()
	}
	began := time.Now()
	close(start)
	wg.Wait()
	final := settle(t, ctx, clients)
	elapsed := time.Since(began)

	var all []time.Duration
	for _, c := range clients {
		all = append(all, c.Latencies()...)
	}
	slices.Sort(all)
	pct := func(p float64) time.Duration { return all[int(p*float64(len(all)-1))] }
	t.Logf("%d editors, %d ops in %s (%.0f ops/s), %d visible runes", editors, len(all), elapsed.Round(time.Millisecond),
		float64(len(all))/elapsed.Seconds(), len([]rune(final)))
	t.Logf("op round trip (send to echo): p50=%s p95=%s p99=%s max=%s", pct(0.50), pct(0.95), pct(0.99), all[len(all)-1])

	if len(all) != editors*edits {
		t.Fatalf("got %d echoes, want %d", len(all), editors*edits)
	}
	if stored, _ := loadText(t, ctx, s.docs, docID); stored != final {
		t.Fatal("document-service state differs from the 100 clients")
	}
}
