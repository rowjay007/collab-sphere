package e2e

import (
	"context"
	"math/rand/v2"
	"sync"
	"testing"
	"time"

	"github.com/rowjay007/collab-sphere/pkg/collabclient"
	"github.com/rowjay007/collab-sphere/pkg/crdt"
	"github.com/rowjay007/collab-sphere/tests/internal/stack"
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

func TestTwoClientsConvergeAndSurviveRestarts(t *testing.T) {
	s := stack.New(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	docID := s.CreateDocument(ctx)

	alice := s.Dial(ctx, docID, 1001)
	bob := s.Dial(ctx, docID, 1002)
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
	sameSpot := stack.Settle(t, ctx, clients)
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
	final := stack.Settle(t, ctx, clients)
	t.Logf("converged on %d visible runes after 700 ops", len([]rune(final)))

	stored, meta := stack.LoadText(t, ctx, s.Docs, docID)
	if stored != final {
		t.Fatalf("document-service state differs from clients:\n got %q\nwant %q", stored, final)
	}
	if meta.GetOpSeq() != 700 {
		t.Fatalf("op_seq = %d, want 700", meta.GetOpSeq())
	}
	for i, c := range clients {
		if c.Seq() != meta.GetOpSeq() {
			t.Fatalf("client %d Seq() = %d, want op_seq %d", i, c.Seq(), meta.GetOpSeq())
		}
	}

	alice.Close()
	bob.Close()
	s.StopHub()
	s.StopDocumentService()
	s.StartDocumentService()
	s.StartHub()

	_, meta = stack.LoadText(t, ctx, s.Docs, docID)
	if meta.GetOpSeq()-meta.GetSnapshotSeq() >= 100 || meta.GetSnapshotSeq() == 0 {
		t.Fatalf("snapshot_seq = %d with op_seq %d breaks the 100-op cadence", meta.GetSnapshotSeq(), meta.GetOpSeq())
	}

	carol := s.Dial(ctx, docID, 1003)
	if got := carol.Text(); got != final {
		t.Fatalf("after restarting both services, DocumentSync text differs:\n got %q\nwant %q", got, final)
	}
	if err := carol.InsertAt(0, '!'); err != nil {
		t.Fatal(err)
	}
	if err := carol.WaitAcked(ctx); err != nil {
		t.Fatal(err)
	}
	if stored, _ := stack.LoadText(t, ctx, s.Docs, docID); stored != "!"+final {
		t.Fatalf("edit after restart not persisted: %q", stored)
	}
}
