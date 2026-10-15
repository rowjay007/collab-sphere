package presence

import (
	"context"
	"sync"

	presencev1 "github.com/rowjay007/collab-sphere/gen/presence/v1"
	"github.com/rowjay007/collab-sphere/pkg/crdt"
)

// Local is presence for a single instance. It keeps the same versions and
// events as presence-service, delivered to this process only, and has no
// TTL: a process death takes its peers with it.
type Local struct {
	deliver func(docID string, ev *presencev1.Event)

	mu   sync.Mutex
	live map[key]*entry
	// versions outlive their documents' sessions, so a reopened document
	// never reuses a version a client still remembers as a tombstone.
	versions map[string]uint64
}

// NewLocal delivers every event through deliver, normally fanout.Bus.Loopback.
// deliver runs under Local's lock, so events reach it in version order.
func NewLocal(deliver func(docID string, ev *presencev1.Event)) *Local {
	return &Local{deliver: deliver, live: make(map[key]*entry), versions: make(map[string]uint64)}
}

// Join leaves a joined session in place when s may not replace it, as
// presence-service does, and still returns everyone else.
func (l *Local) Join(_ context.Context, docID string, s Session) ([]*presencev1.State, uint64, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	k := key{doc: docID, client: s.Client}
	if l.admits(k, s) {
		e := hold(l.live, k, s)
		e.phase = joined
		l.publish(k, e)
	}
	var others []*presencev1.State
	for ok, oe := range l.live {
		if ok.doc == docID && ok.client != s.Client && oe.phase == joined {
			others = append(others, oe.state(ok))
		}
	}
	return others, l.versions[docID], nil
}

func (l *Local) admits(k key, s Session) bool {
	e := l.live[k]
	return e == nil || e.phase != joined || e.yields(s)
}

func (l *Local) Move(docID string, s Session, anchor, head crdt.ID) {
	l.update(docID, s, func(e *entry) {
		e.anchor, e.head, e.hasCursor = anchor, head, true
		e.name = s.Name
	})
}

func (l *Local) Rename(docID string, s Session, name string) {
	l.update(docID, s, func(e *entry) { e.name = name })
}

func (l *Local) update(docID string, s Session, fn func(*entry)) {
	l.mu.Lock()
	defer l.mu.Unlock()
	k := key{doc: docID, client: s.Client}
	if !l.admits(k, s) {
		return
	}
	e := hold(l.live, k, s)
	fn(e)
	if e.phase == joined {
		l.publish(k, e)
	}
}

func (l *Local) Leave(docID string, s Session) {
	l.mu.Lock()
	defer l.mu.Unlock()
	k := key{doc: docID, client: s.Client}
	e := l.live[k]
	if e == nil || e.session != s.Session {
		return
	}
	delete(l.live, k)
	if e.phase != joined {
		return
	}
	l.versions[docID]++
	l.deliver(docID, &presencev1.Event{
		DocumentId: docID,
		Client:     uint64(s.Client),
		Version:    l.versions[docID],
		Kind: &presencev1.Event_Left{Left: &presencev1.Left{
			Session: s.Session,
			Reason:  presencev1.Left_REASON_LEAVE,
		}},
	})
}

func (l *Local) List(_ context.Context, docID string) ([]*presencev1.State, uint64, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	var states []*presencev1.State
	for k, e := range l.live {
		if k.doc == docID && e.phase == joined {
			states = append(states, e.state(k))
		}
	}
	return states, l.versions[docID], nil
}

func (l *Local) publish(k key, e *entry) {
	l.versions[k.doc]++
	e.version = l.versions[k.doc]
	l.deliver(k.doc, &presencev1.Event{
		DocumentId: k.doc,
		Client:     uint64(k.client),
		Version:    e.version,
		Kind:       &presencev1.Event_State{State: e.state(k)},
	})
}
