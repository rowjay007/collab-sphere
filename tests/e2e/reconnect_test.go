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

// tracked runs one edit and returns the IDs of the ops it made.
func tracked(c *collabclient.Client, fn func(r *crdt.Replica) (crdt.Op, error)) ([]crdt.ID, error) {
	var ids []crdt.ID
	err := c.Edit(func(r *crdt.Replica) ([]crdt.Op, error) {
		op, err := fn(r)
		if err != nil {
			return nil, err
		}
		ids = append(ids, op.OpID())
		return []crdt.Op{op}, nil
	})
	return ids, err
}

func typeOrDelete(rng *rand.Rand) func(r *crdt.Replica) (crdt.Op, error) {
	return func(r *crdt.Replica) (crdt.Op, error) {
		if r.Len() > 0 && rng.IntN(10) < 3 {
			return r.DeleteAt(rng.IntN(r.Len()))
		}
		return r.InsertAt(rng.IntN(r.Len()+1), rune(letters[rng.IntN(len(letters))]))
	}
}

// typeOrDeleteOwn inserts, or deletes a visible char it inserted itself.
func typeOrDeleteOwn(rng *rand.Rand, own map[crdt.ID]bool) func(r *crdt.Replica) (crdt.Op, error) {
	return func(r *crdt.Replica) (crdt.Op, error) {
		if len(own) > 0 && rng.IntN(10) < 3 {
			var visible []crdt.ID
			for i := range r.Len() {
				if id, err := r.IDAt(i); err == nil && own[id] {
					visible = append(visible, id)
				}
			}
			if len(visible) > 0 {
				target := visible[rng.IntN(len(visible))]
				delete(own, target)
				return r.Delete(target)
			}
		}
		op, err := r.InsertAt(rng.IntN(r.Len()+1), rune(letters[rng.IntN(len(letters))]))
		if err == nil {
			own[op.ID] = true
		}
		return op, err
	}
}

func TestOfflineEditsResumeExactlyOnce(t *testing.T) {
	s := stack.New(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	docID := s.CreateDocument(ctx)

	x := s.Dial(ctx, docID, 2001)
	y := s.Dial(ctx, docID, 2002)
	clients := []*collabclient.Client{x, y}
	rngX, rngY := rand.New(rand.NewPCG(1, 42)), rand.New(rand.NewPCG(2, 42))

	var generated []crdt.ID
	for range 20 {
		ids, err := tracked(x, typeOrDelete(rngX))
		if err != nil {
			t.Fatal(err)
		}
		generated = append(generated, ids...)
	}
	stack.Settle(t, ctx, clients)

	x.Disconnect()
	var offline, online []crdt.ID
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		own := make(map[crdt.ID]bool)
		for range 50 {
			ids, err := tracked(x, typeOrDeleteOwn(rngX, own))
			if err != nil {
				t.Error(err)
				return
			}
			offline = append(offline, ids...)
		}
	}()
	go func() {
		defer wg.Done()
		for range 50 {
			ids, err := tracked(y, typeOrDelete(rngY))
			if err != nil {
				t.Error(err)
				return
			}
			online = append(online, ids...)
		}
	}()
	wg.Wait()
	if got := x.Unacked(); got != 50 {
		t.Fatalf("X has %d unacked ops after 50 offline edits", got)
	}

	if err := x.Reconnect(ctx, s.HubURL()); err != nil {
		t.Fatal(err)
	}
	if got := x.LastJoin(); got != collabclient.Missed {
		t.Fatalf("LastJoin() = %v, want Missed", got)
	}
	final := stack.Settle(t, ctx, clients)
	for i, c := range clients {
		if n := c.Unacked(); n != 0 {
			t.Fatalf("client %d has %d unacked ops after Settle", i, n)
		}
	}
	generated = append(append(generated, offline...), online...)
	stack.AssertExactlyOnce(t, ctx, s.Docs, docID, generated)
	if stored, _ := stack.LoadText(t, ctx, s.Docs, docID); stored != final {
		t.Fatalf("stored text differs from clients:\n got %q\nwant %q", stored, final)
	}
}
