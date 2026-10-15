package fanout

import (
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	presencev1 "github.com/rowjay007/collab-sphere/gen/presence/v1"
	"github.com/rowjay007/collab-sphere/pkg/crdt"
)

type recorder struct {
	batches chan Batch
	events  chan *presencev1.Event
	resyncs chan struct{}
}

func newRecorder() *recorder {
	return &recorder{
		batches: make(chan Batch, 64),
		events:  make(chan *presencev1.Event, 64),
		resyncs: make(chan struct{}, 64),
	}
}

func (r *recorder) RemoteOps(b Batch)             { r.batches <- b }
func (r *recorder) Presence(ev *presencev1.Event) { r.events <- ev }
func (r *recorder) Resync()                       { r.resyncs <- struct{}{} }
func (r *recorder) expectNoBatch(t *testing.T, d time.Duration) {
	t.Helper()
	select {
	case b := <-r.batches:
		t.Fatalf("unexpected batch at seq %d", b.FirstSeq)
	case <-time.After(d):
	}
}

func (r *recorder) nextBatch(t *testing.T) Batch {
	t.Helper()
	select {
	case b := <-r.batches:
		return b
	case <-time.After(5 * time.Second):
		t.Fatal("no batch arrived")
		return Batch{}
	}
}

func batchAt(seq uint64) Batch {
	return Batch{FirstSeq: seq, Ops: []crdt.Op{crdt.Insert{ID: crdt.ID{Client: 1, Clock: seq}, Parent: crdt.Root, Value: 'x'}}}
}

func TestMemoryDropsDelaysAndCloses(t *testing.T) {
	m := NewMemory()
	a, b := newRecorder(), newRecorder()
	subA := m.Join("doc", a)
	m.Join("doc", b)
	select {
	case <-subA.Ready():
	default:
		t.Fatal("memory subscription is not ready at once")
	}
	m.Publish("nobody", batchAt(1))

	m.SetFault(func(_ string, b Batch) (bool, time.Duration) {
		switch b.FirstSeq {
		case 1:
			return true, 0
		case 2:
			return false, 20 * time.Millisecond
		}
		return false, 0
	})
	for seq := uint64(1); seq <= 3; seq++ {
		m.Publish("doc", batchAt(seq))
	}
	for _, r := range []*recorder{a, b} {
		if got := []uint64{r.nextBatch(t).FirstSeq, r.nextBatch(t).FirstSeq}; got[0] != 3 || got[1] != 2 {
			t.Fatalf("delivered seqs %v, want [3 2]", got)
		}
	}

	subA.Close()
	subA.Close()
	m.Publish("doc", batchAt(4))
	m.Resync("doc")
	if got := b.nextBatch(t).FirstSeq; got != 4 {
		t.Fatalf("delivered seq %d, want 4", got)
	}
	<-b.resyncs
	a.expectNoBatch(t, 50*time.Millisecond)
	if len(a.resyncs) != 0 {
		t.Fatal("closed sink got a resync")
	}
}

func TestRedisPublishNeverBlocks(t *testing.T) {
	r := NewRedis(redis.NewClient(&redis.Options{Addr: "127.0.0.1:1"}), "stalled")
	done := make(chan struct{})
	go func() {
		defer close(done)
		for seq := range uint64(3 * publishQueue) {
			r.Publish("doc", batchAt(seq+1))
		}
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Publish blocked on a full queue")
	}
}
