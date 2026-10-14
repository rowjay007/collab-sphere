package load

import (
	"context"
	"math/rand/v2"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/rowjay007/collab-sphere/pkg/collabclient"
	"github.com/rowjay007/collab-sphere/pkg/crdt"
	"github.com/rowjay007/collab-sphere/tests/internal/stack"
)

const letters = "abcdefghijklmnopqrstuvwxyz "

// typist edits like a person at a cursor held as a CRDT ID: it types after the
// cursor and backspaces over its own runs, so no edit walks the document.
type typist struct {
	anchor crdt.ID
	typed  []crdt.ID
}

func newTypist(rng *rand.Rand, c *collabclient.Client) (*typist, error) {
	ty := &typist{anchor: crdt.Root}
	var err error
	c.Read(func(r *crdt.Replica) {
		if n := r.Len(); n > 0 {
			ty.anchor, err = r.IDAt(rng.IntN(n))
		}
	})
	return ty, err
}

func (ty *typist) edit(rng *rand.Rand, c *collabclient.Client) error {
	return c.Edit(func(r *crdt.Replica) ([]crdt.Op, error) {
		if len(ty.typed) > 0 && rng.IntN(10) < 2 {
			op, err := r.Delete(ty.typed[len(ty.typed)-1])
			ty.typed = ty.typed[:len(ty.typed)-1]
			return []crdt.Op{op}, err
		}
		cursor := ty.anchor
		if len(ty.typed) > 0 {
			cursor = ty.typed[len(ty.typed)-1]
		}
		op, err := r.InsertAfter(cursor, rune(letters[rng.IntN(len(letters))]))
		ty.typed = append(ty.typed, op.ID)
		return []crdt.Op{op}, err
	})
}

func TestHundredConcurrentEditors(t *testing.T) {
	if testing.Short() {
		t.Skip("load test")
	}
	s := stack.New(t)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	docID := s.CreateDocument(ctx)

	const editors, edits = 100, 40
	seeder := s.Dial(ctx, docID, 4000)
	for remaining := 20_000; remaining > 0; {
		count := min(remaining, 500)
		if err := seeder.Edit(func(r *crdt.Replica) ([]crdt.Op, error) {
			inserts, err := r.InsertText(r.Len(), strings.Repeat("s", count))
			ops := make([]crdt.Op, len(inserts))
			for i, op := range inserts {
				ops[i] = op
			}
			return ops, err
		}); err != nil {
			t.Fatal(err)
		}
		remaining -= count
	}
	if err := seeder.WaitAcked(ctx); err != nil {
		t.Fatal(err)
	}
	if err := seeder.Close(); err != nil {
		t.Fatal(err)
	}

	clients := make([]*collabclient.Client, editors)
	for i := range clients {
		clients[i] = s.Dial(ctx, docID, crdt.ClientID(5000+i))
	}

	var wg sync.WaitGroup
	start := make(chan struct{})
	for i, c := range clients {
		wg.Add(1)
		go func() {
			defer wg.Done()
			rng := rand.New(rand.NewPCG(uint64(i), 7))
			typist, err := newTypist(rng, c)
			if err != nil {
				t.Error(err)
				return
			}
			<-start
			for range edits {
				time.Sleep(time.Duration(100+rng.IntN(100)) * time.Millisecond)
				if err := typist.edit(rng, c); err != nil {
					t.Error(err)
					return
				}
			}
		}()
	}
	began := time.Now()
	close(start)
	wg.Wait()
	final := stack.Settle(t, ctx, clients)
	elapsed := time.Since(began)

	var echo, fanout []time.Duration
	sent := make(map[crdt.ID]time.Time, editors*edits)
	for _, c := range clients {
		echo = append(echo, c.Latencies()...)
		for id, at := range c.SendTimes() {
			sent[id] = at
		}
	}
	received := make([]map[crdt.ID]time.Time, len(clients))
	for i, c := range clients {
		received[i] = c.ReceiveTimes()
	}
	for id, sentAt := range sent {
		var last time.Time
		for i, c := range clients {
			var clientID crdt.ClientID
			c.Read(func(r *crdt.Replica) { clientID = r.Client() })
			if clientID == id.Client {
				continue
			}
			at, ok := received[i][id]
			if !ok {
				t.Fatalf("client %d has no receive time for %v", i, id)
			}
			if at.After(last) {
				last = at
			}
		}
		fanout = append(fanout, last.Sub(sentAt))
	}
	slices.Sort(echo)
	slices.Sort(fanout)
	pct := func(values []time.Duration, p float64) time.Duration {
		return values[int(p*float64(len(values)-1))]
	}
	t.Logf("%d editors, %d ops in %s (%.0f ops/s), %d visible runes", editors, len(echo), elapsed.Round(time.Millisecond),
		float64(len(echo))/elapsed.Seconds(), len([]rune(final)))
	t.Logf("echo latency: p50=%s p95=%s p99=%s max=%s", pct(echo, 0.50), pct(echo, 0.95), pct(echo, 0.99), echo[len(echo)-1])
	t.Logf("fan-out latency: p50=%s p95=%s p99=%s max=%s", pct(fanout, 0.50), pct(fanout, 0.95), pct(fanout, 0.99), fanout[len(fanout)-1])

	if len(echo) != editors*edits {
		t.Fatalf("got %d echoes, want %d", len(echo), editors*edits)
	}
	if len(fanout) != editors*edits {
		t.Fatalf("got %d fan-out measurements, want %d", len(fanout), editors*edits)
	}
	if stored, _ := stack.LoadText(t, ctx, s.Docs, docID); stored != final {
		t.Fatal("document-service state differs from the 100 clients")
	}
}
