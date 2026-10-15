package hub

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/rowjay007/collab-sphere/pkg/crdt"
)

// Registry maps document IDs to live hubs and counts connected clients.
type Registry struct {
	store    Store
	gapGrace time.Duration
	mu       sync.Mutex
	hubs     map[string]*Hub
}

// NewRegistry starts with no hubs. The first join for a document creates one.
func NewRegistry(store Store) *Registry {
	return &Registry{
		store:    store,
		gapGrace: defaultGapGrace,
		hubs:     make(map[string]*Hub),
	}
}

// Session is one client's attachment to the hub it joined. Every later
// message goes to that hub, even if the registry has since replaced it.
type Session struct {
	reg  *Registry
	hub  *Hub
	peer *Peer
}

// Join attaches peer to docID. The caller must Leave the returned session on disconnect.
func (r *Registry) Join(ctx context.Context, docID string, peer *Peer) (*Session, error) {
	for {
		h := r.acquire(docID)
		reply := make(chan error, 1)
		if err := h.send(ctx, joinEvent{peer: peer, reply: reply}); err != nil {
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
	_ = s.hub.send(ctx, cursorEvent{id: s.peer.id, anchor: anchor, head: head})
}

// Leave detaches the client and stops the hub when it is empty.
func (s *Session) Leave(ctx context.Context) {
	reply := make(chan struct{})
	if err := s.hub.send(ctx, leaveEvent{peer: s.peer, reply: reply}); err == nil {
		_, _ = waitReply(ctx, s.hub, reply)
	}
	s.reg.release(s.hub)
}

// Close stops every hub. In-flight joins fail with ErrStopped.
func (r *Registry) Close() {
	r.mu.Lock()
	hubs := make([]*Hub, 0, len(r.hubs))
	for _, h := range r.hubs {
		hubs = append(hubs, h)
	}
	r.hubs = make(map[string]*Hub)
	r.mu.Unlock()
	for _, h := range hubs {
		_ = h.send(context.Background(), stopEvent{})
	}
}

func (r *Registry) acquire(docID string) *Hub {
	r.mu.Lock()
	defer r.mu.Unlock()
	if h, ok := r.hubs[docID]; ok {
		h.refs++
		return h
	}
	h := newHub(docID, r)
	h.refs = 1
	r.hubs[docID] = h
	go h.loop()
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
