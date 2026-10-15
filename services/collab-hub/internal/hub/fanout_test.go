package hub

import (
	"context"
	"math/rand/v2"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/rowjay007/collab-sphere/pkg/crdt"
	"github.com/rowjay007/collab-sphere/services/collab-hub/internal/fanout"
	"github.com/rowjay007/collab-sphere/services/collab-hub/internal/presence"
)

func registries(t *testing.T, store *MemoryStore, bus fanout.Bus, n int) []*Registry {
	t.Helper()
	pres := presence.NewLocal(bus.Loopback)
	regs := make([]*Registry, n)
	for i := range regs {
		regs[i] = NewRegistry(store, bus, pres)
		t.Cleanup(regs[i].Close)
	}
	return regs
}

// typist edits the way a client does. Its replica is what its stream has
// delivered plus its own ops, so every op it sends is valid at its hub.
type typist struct {
	session *Session
	stream  *stream
	replica *crdt.Replica
}

func (ty *typist) edit(ctx context.Context, sent *atomic.Uint64) error {
	_, _, seen, _ := ty.stream.snapshot()
	if err := ty.replica.Merge(seen); err != nil {
		return err
	}
	batch := make([]crdt.Op, 0, 3)
	for range 1 + rand.IntN(3) {
		var op crdt.Op
		var err error
		if n := ty.replica.Len(); n > 0 && rand.IntN(4) == 0 {
			op, err = ty.replica.DeleteAt(rand.IntN(n))
		} else {
			op, err = ty.replica.InsertAt(rand.IntN(ty.replica.Len()+1), rune('a'+rand.IntN(26)))
		}
		if err != nil {
			return err
		}
		batch = append(batch, op)
	}
	sent.Add(uint64(len(batch)))
	ty.session.SubmitOps(ctx, batch)
	return nil
}

// editTogether joins clients round-robin across regs and has each send
// edits batches concurrently.
func editTogether(t *testing.T, ctx context.Context, regs []*Registry, clients, edits int, sent *atomic.Uint64) []*typist {
	t.Helper()
	typists := make([]*typist, clients)
	for i := range typists {
		id := crdt.ClientID(i + 1)
		session, _, s := join(t, ctx, regs[i%len(regs)], id)
		replica, err := crdt.NewReplica(id, nil)
		if err != nil {
			t.Fatal(err)
		}
		typists[i] = &typist{session: session, stream: s, replica: replica}
	}
	var wg sync.WaitGroup
	errs := make(chan error, clients)
	for _, ty := range typists {
		wg.Go(func() {
			for range edits {
				if err := ty.edit(ctx, sent); err != nil {
					errs <- err
					return
				}
				if rand.IntN(3) == 0 {
					time.Sleep(time.Duration(rand.IntN(2000)) * time.Microsecond)
				}
			}
		})
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
	return typists
}

func streamsOf(typists []*typist) []*stream {
	streams := make([]*stream, len(typists))
	for i, ty := range typists {
		streams[i] = ty.stream
	}
	return streams
}

func waitAll(t *testing.T, ctx context.Context, streams []*stream, seq uint64) {
	t.Helper()
	for i, s := range streams {
		if err := s.waitSeq(ctx, seq); err != nil {
			t.Fatalf("stream %d: %v", i, err)
		}
	}
}

func TestFanoutConvergesAcrossRegistries(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	store := NewMemoryStore()
	regs := registries(t, store, fanout.NewMemory(), 3)
	for _, r := range regs {
		r.idleSync = time.Hour
		r.gapGrace = time.Hour
	}

	var sent atomic.Uint64
	streams := streamsOf(editTogether(t, ctx, regs, 30, 20, &sent))
	waitAll(t, ctx, streams, sent.Load())
	assertConverged(t, store, streams...)
}

func TestFanoutConvergesThroughLossAndReorder(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	store, bus := NewMemoryStore(), fanout.NewMemory()
	regs := registries(t, store, bus, 3)
	for _, r := range regs {
		r.idleSync = time.Hour
	}
	bus.SetFault(func(string, fanout.Batch) (bool, time.Duration) {
		switch p := rand.Float64(); {
		case p < 0.2:
			return true, 0
		case p < 0.5:
			return false, time.Duration(rand.IntN(5000)) * time.Microsecond
		}
		return false, 0
	})

	var sent atomic.Uint64
	typists := editTogether(t, ctx, regs, 30, 20, &sent)
	bus.SetFault(nil)
	// A dropped final batch leaves no gap behind it. One more edit per
	// registry, appended after the loss stopped, exposes it to gap fill.
	for _, ty := range typists[:len(regs)] {
		if err := ty.edit(ctx, &sent); err != nil {
			t.Fatal(err)
		}
	}
	streams := streamsOf(typists)
	waitAll(t, ctx, streams, sent.Load())
	assertConverged(t, store, streams...)
}

func TestIdleSyncHealsLostFinalMessage(t *testing.T) {
	const idle = 200 * time.Millisecond
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	store, bus := NewMemoryStore(), fanout.NewMemory()
	regs := registries(t, store, bus, 3)
	for _, r := range regs {
		r.idleSync = idle
	}
	alice, _, aliceStream := join(t, ctx, regs[0], 1)
	_, _, bobStream := join(t, ctx, regs[1], 2)
	_, _, carolStream := join(t, ctx, regs[2], 3)
	streams := []*stream{aliceStream, bobStream, carolStream}

	alice.SubmitOps(ctx, []crdt.Op{insert(1, 1)})
	waitAll(t, ctx, streams, 1)
	bus.SetFault(func(string, fanout.Batch) (bool, time.Duration) { return true, 0 })
	alice.SubmitOps(ctx, []crdt.Op{insert(1, 2)})
	if err := aliceStream.waitSeq(ctx, 2); err != nil {
		t.Fatal(err)
	}

	healCtx, healCancel := context.WithTimeout(ctx, idle+300*time.Millisecond)
	defer healCancel()
	waitAll(t, healCtx, streams[1:], 2)
	assertConverged(t, store, streams...)
}

func TestResyncFillsAtOnce(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	store, bus := NewMemoryStore(), fanout.NewMemory()
	regs := registries(t, store, bus, 2)
	for _, r := range regs {
		r.idleSync = time.Hour
		r.gapGrace = time.Hour
	}
	_, _, aliceStream := join(t, ctx, regs[0], 1)
	_, _, bobStream := join(t, ctx, regs[1], 2)

	if _, err := store.AppendFromOtherWriter("doc", insert(50, 1)); err != nil {
		t.Fatal(err)
	}
	bus.Resync("doc")
	fillCtx, fillCancel := context.WithTimeout(ctx, 200*time.Millisecond)
	defer fillCancel()
	waitAll(t, fillCtx, []*stream{aliceStream, bobStream}, 1)
	assertConverged(t, store, aliceStream, bobStream)
}

func TestFullPublishQueueNeverBlocksHub(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	// Nothing runs this bus, so its subscription never confirms and its
	// publish queue fills and stays full.
	bus := fanout.NewRedis(redis.NewClient(&redis.Options{Addr: "127.0.0.1:1"}), "stalled")
	store := NewMemoryStore()
	reg := NewRegistry(store, bus, presence.NewLocal(bus.Loopback))
	reg.readyWait = 10 * time.Millisecond
	t.Cleanup(reg.Close)
	alice, _, aliceStream := join(t, ctx, reg, 1)

	const appends = 1500
	for clock := uint64(1); clock <= appends; clock++ {
		alice.SubmitOps(ctx, []crdt.Op{insert(1, clock)})
		if err := aliceStream.waitSeq(ctx, clock); err != nil {
			t.Fatal(err)
		}
	}
	assertConverged(t, store, aliceStream)
}
