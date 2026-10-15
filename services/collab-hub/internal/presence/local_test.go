package presence

import (
	"context"
	"sync"
	"testing"

	presencev1 "github.com/rowjay007/collab-sphere/gen/presence/v1"
)

type recorder struct {
	mu     sync.Mutex
	events []*presencev1.Event
}

func (r *recorder) deliver(_ string, ev *presencev1.Event) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.events = append(r.events, ev)
}

func (r *recorder) count() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.events)
}

func TestLocalLowerEpochJoinDoesNotEvict(t *testing.T) {
	var rec recorder
	l := NewLocal(rec.deliver)
	ctx := context.Background()
	newer := Session{Client: 7, Session: 2002, Epoch: 2}
	older := Session{Client: 7, Session: 1001, Epoch: 1}
	if _, _, err := l.Join(ctx, "doc", newer); err != nil {
		t.Fatal(err)
	}
	before := rec.count()
	if _, _, err := l.Join(ctx, "doc", older); err != nil {
		t.Fatal(err)
	}
	l.Move("doc", older, at(1), at(1))
	l.Rename("doc", older, "stale")
	l.Leave("doc", older)
	if n := rec.count() - before; n != 0 {
		t.Fatalf("the older session published %d events", n)
	}
	states, _, _ := l.List(ctx, "doc")
	if len(states) != 1 || states[0].GetSession() != newer.Session {
		t.Fatalf("List = %v, want the newer session", states)
	}
}

func TestLocalEpochZeroIsLastWriterWins(t *testing.T) {
	var rec recorder
	l := NewLocal(rec.deliver)
	ctx := context.Background()
	for _, s := range []Session{{Client: 7, Session: 1}, {Client: 7, Session: 2}} {
		if _, _, err := l.Join(ctx, "doc", s); err != nil {
			t.Fatal(err)
		}
	}
	states, _, _ := l.List(ctx, "doc")
	if len(states) != 1 || states[0].GetSession() != 2 {
		t.Fatalf("List = %v, want the later session", states)
	}
}
