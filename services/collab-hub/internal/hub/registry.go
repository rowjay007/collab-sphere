package hub

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/rowjay007/collab-sphere/pkg/crdt"
	"github.com/rowjay007/collab-sphere/services/collab-hub/internal/fanout"
)

// Registry maps document IDs to live hubs and counts connected clients.
type Registry struct {
	store     Store
	bus       fanout.Bus
	pres      Presence
	gapGrace  time.Duration
	idleSync  time.Duration
	readyWait time.Duration
	maxReplay uint64
	mu        sync.Mutex
	hubs      map[string]*Hub
	closed    bool

	loops         sync.WaitGroup
	presenceCalls sync.WaitGroup
}

// NewRegistry starts with no hubs. The first join for a document creates one,
// which learns about other instances' appends and presence through bus.
func NewRegistry(store Store, bus fanout.Bus, pres Presence) *Registry {
	return &Registry{
		store:     store,
		bus:       bus,
		pres:      pres,
		gapGrace:  defaultGapGrace,
		idleSync:  defaultIdleSync,
		readyWait: defaultReadyWait,
		maxReplay: defaultMaxReplay,
		hubs:      make(map[string]*Hub),
	}
}

// Session is one client's attachment to the hub it joined. Every later
// message goes to that hub, even if the registry has since replaced it.
type Session struct {
	reg  *Registry
	hub  *Hub
	peer *Peer
}

// Join attaches peer to docID. A resuming client holds log[1..seq] and gets
// MissedOps instead of a DocumentSync when it is close enough. The caller
// must Leave the returned session on disconnect. After Close it returns
// ErrStopped.
func (r *Registry) Join(ctx context.Context, docID string, peer *Peer, resume bool, seq uint64) (*Session, error) {
	for {
		h := r.acquire(docID)
		if h == nil {
			return nil, ErrStopped
		}
		reply := make(chan error, 1)
		if err := h.send(ctx, joinEvent{peer: peer, resume: resume, seq: seq, reply: reply}); err != nil {
			r.release(h)
			if err == ErrStopped {
				continue
			}
			return nil, err
		}
		err, waitErr := waitReply(ctx, h, reply)
		if waitErr == nil {
			waitErr = err
		}
		if waitErr != nil {
			if errors.Is(waitErr, context.Canceled) || errors.Is(waitErr, context.DeadlineExceeded) {
				reply := make(chan struct{})
				if h.send(context.Background(), leaveEvent{peer: peer, reply: reply}) == nil {
					_, _ = waitReply(context.Background(), h, reply)
				}
			}
			r.release(h)
			if waitErr == ErrStopped {
				continue
			}
			return nil, waitErr
		}
		return &Session{reg: r, hub: h, peer: peer}, nil
	}
}

// SubmitOps delivers an already-parsed operation batch to the session's hub.
func (s *Session) SubmitOps(ctx context.Context, ops []crdt.Op) {
	_ = s.hub.send(ctx, opsEvent{id: s.peer.id, ops: ops})
}

// MoveCursor delivers a cursor update to the session's hub.
func (s *Session) MoveCursor(ctx context.Context, anchor, head crdt.ID) {
	_ = s.hub.send(ctx, cursorEvent{peer: s.peer, anchor: anchor, head: head})
}

// Rename sets the display name the session's presence carries.
func (s *Session) Rename(ctx context.Context, name string) {
	_ = s.hub.send(ctx, renameEvent{peer: s.peer, name: name})
}

// Leave detaches the client and stops the hub when it is empty.
func (s *Session) Leave(ctx context.Context) {
	reply := make(chan struct{})
	if err := s.hub.send(ctx, leaveEvent{peer: s.peer, reply: reply}); err == nil {
		_, _ = waitReply(ctx, s.hub, reply)
	}
	s.reg.release(s.hub)
}

// Close refuses later joins, stops every hub, and returns once each has
// closed its peers and handed every presence Leave to Presence. In-flight
// joins fail with ErrStopped.
func (r *Registry) Close() {
	r.mu.Lock()
	r.closed = true
	hubs := make([]*Hub, 0, len(r.hubs))
	for _, h := range r.hubs {
		hubs = append(hubs, h)
	}
	r.hubs = make(map[string]*Hub)
	r.mu.Unlock()
	for _, h := range hubs {
		_ = h.send(context.Background(), stopEvent{})
	}
	r.loops.Wait()
	r.presenceCalls.Wait()
}

// acquire returns docID's hub, starting one if needed, or nil once the
// registry is closed.
func (r *Registry) acquire(docID string) *Hub {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return nil
	}
	if h, ok := r.hubs[docID]; ok {
		h.refs++
		return h
	}
	h := newHub(docID, r)
	h.refs = 1
	r.hubs[docID] = h
	r.loops.Go(h.loop)
	return h
}

func (r *Registry) release(h *Hub) {
	r.mu.Lock()
	if r.hubs[h.docID] != h {
		r.mu.Unlock()
		return
	}
	h.refs--
	if h.refs > 0 {
		r.mu.Unlock()
		return
	}
	delete(r.hubs, h.docID)
	r.mu.Unlock()
	_ = h.send(context.Background(), stopEvent{})
}

func (r *Registry) evict(h *Hub) {
	r.mu.Lock()
	if r.hubs[h.docID] == h {
		delete(r.hubs, h.docID)
	}
	r.mu.Unlock()
}
