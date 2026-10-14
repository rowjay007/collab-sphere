package hub

import (
	"context"
	"errors"
	"sync"
	"time"

	collabv1 "github.com/rowjay007/collab-sphere/gen/collab/v1"
	crdtv1 "github.com/rowjay007/collab-sphere/gen/crdt/v1"
	"github.com/rowjay007/collab-sphere/pkg/crdt"
)

const (
	appendTimeout  = 5 * time.Second
	loadTimeout    = 10 * time.Second
	maxOpsPerGroup = 4096
)

var (
	// ErrDuplicateClient is returned when that ClientID is already on the document.
	ErrDuplicateClient = errors.New("client already connected")
	// ErrStopped is returned when a request lands on a hub that is shutting down.
	ErrStopped = errors.New("hub stopped")
)

type event interface{ isEvent() }

type joinEvent struct {
	peer  *Peer
	reply chan error
}

func (joinEvent) isEvent() {}

type leaveEvent struct {
	peer  *Peer
	reply chan struct{}
}

func (leaveEvent) isEvent() {}

type opsEvent struct {
	id  crdt.ClientID
	ops []crdt.Op
}

func (opsEvent) isEvent() {}

type cursorEvent struct {
	id     crdt.ClientID
	anchor crdt.ID
	head   crdt.ID
}

func (cursorEvent) isEvent() {}

type stopEvent struct{}

func (stopEvent) isEvent() {}

type cursor struct {
	anchor crdt.ID
	head   crdt.ID
}

type Hub struct {
	docID    string
	store    Store
	registry *Registry
	events   chan event
	stopped  chan struct{}
	once     sync.Once
	refs     int

	doc     *crdt.Doc
	loadErr error
	peers   map[crdt.ClientID]*Peer
	cursors map[crdt.ClientID]cursor
	dead    bool
}

func newHub(docID string, store Store, registry *Registry) *Hub {
	h := &Hub{
		docID:    docID,
		store:    store,
		registry: registry,
		events:   make(chan event, 256),
		stopped:  make(chan struct{}),
		peers:    make(map[crdt.ClientID]*Peer),
		cursors:  make(map[crdt.ClientID]cursor),
	}
	return h
}

func (h *Hub) loop() {
	ctx, cancel := context.WithTimeout(context.Background(), loadTimeout)
	doc, err := h.store.Load(ctx, h.docID)
	cancel()
	if err != nil {
		h.loadErr = err
	} else {
		h.doc = doc
	}

	var pending event
	for {
		var ev event
		if pending != nil {
			ev, pending = pending, nil
		} else {
			ev = <-h.events
		}
		switch ev := ev.(type) {
		case stopEvent:
			h.shutdown()
			return
		case joinEvent:
			h.handleJoin(ev)
		case leaveEvent:
			h.handleLeave(ev)
		case opsEvent:
			var failed bool
			pending, failed = h.handleOpsGroup(ev)
			if failed {
				h.shutdown()
				return
			}
		case cursorEvent:
			h.handleCursor(ev)
		}
	}
}

func (h *Hub) closeStopped() {
	h.once.Do(func() { close(h.stopped) })
}

func (h *Hub) shutdown() {
	h.closeStopped()
	h.dead = true
	for _, p := range h.peers {
		p.CloseWithReason("hub stopped")
	}
	h.peers = nil
	h.cursors = nil
	for {
		select {
		case ev := <-h.events:
			h.reject(ev)
		default:
			return
		}
	}
}

func (h *Hub) reject(ev event) {
	switch ev := ev.(type) {
	case joinEvent:
		ev.reply <- ErrStopped
	case leaveEvent:
		close(ev.reply)
	}
}

func (h *Hub) handleJoin(ev joinEvent) {
	if h.dead {
		ev.reply <- ErrStopped
		return
	}
	if h.loadErr != nil {
		message := "failed to load document"
		if errors.Is(h.loadErr, ErrArchived) {
			message = "document archived"
		}
		ev.peer.TrySend(EncodeError(collabv1.ErrorMessage_CODE_INTERNAL, message))
		ev.reply <- h.loadErr
		return
	}
	if _, ok := h.peers[ev.peer.id]; ok {
		ev.peer.TrySend(EncodeError(collabv1.ErrorMessage_CODE_CLIENT_MISMATCH, "client already connected"))
		ev.reply <- ErrDuplicateClient
		return
	}

	h.peers[ev.peer.id] = ev.peer
	sync := mustMarshal(&collabv1.ServerMessage{
		Payload: &collabv1.ServerMessage_DocumentSync{DocumentSync: &collabv1.DocumentSync{
			DocumentId: h.docID,
			Snapshot:   h.doc.Snapshot(),
		}},
	})
	if !h.sendOrDrop(ev.peer, sync) {
		ev.reply <- errors.New("send buffer full")
		return
	}
	for id, cur := range h.cursors {
		if id == ev.peer.id {
			continue
		}
		frame := mustMarshal(&collabv1.ServerMessage{
			Payload: &collabv1.ServerMessage_PresenceBroadcast{PresenceBroadcast: &collabv1.PresenceBroadcast{
				Client: uint64(id),
				State:  &collabv1.PresenceBroadcast_Cursor{Cursor: cursorProto(cur)},
			}},
		})
		if !h.sendOrDrop(ev.peer, frame) {
			ev.reply <- errors.New("send buffer full")
			return
		}
	}
	ev.reply <- nil
}

func (h *Hub) handleLeave(ev leaveEvent) {
	h.dropPeer(ev.peer, true)
	close(ev.reply)
}

func (h *Hub) handleOpsGroup(first opsEvent) (event, bool) {
	accepted := h.applyOps(first)
	opCount := len(first.ops)
	for opCount < maxOpsPerGroup {
		select {
		case next := <-h.events:
			ev, ok := next.(opsEvent)
			if !ok {
				return next, h.persistAndBroadcast(accepted)
			}
			if len(ev.ops) > maxOpsPerGroup-opCount {
				return ev, h.persistAndBroadcast(accepted)
			}
			opCount += len(ev.ops)
			accepted = append(accepted, h.applyOps(ev)...)
		default:
			return nil, h.persistAndBroadcast(accepted)
		}
	}
	return nil, h.persistAndBroadcast(accepted)
}

func (h *Hub) applyOps(ev opsEvent) []crdt.Op {
	if h.dead {
		return nil
	}
	p, ok := h.peers[ev.id]
	if !ok {
		return nil
	}
	accepted := make([]crdt.Op, 0, len(ev.ops))
	for _, op := range ev.ops {
		if op.OpID().Client != ev.id {
			h.rejectOperation(p, collabv1.ErrorMessage_CODE_CLIENT_MISMATCH, "operation client does not match connection")
			break
		}
		clock := op.OpID().Clock
		docClock := h.doc.Clock()
		if clock > docClock && clock-docClock > 1 {
			h.rejectOperation(p, collabv1.ErrorMessage_CODE_INVALID_OPERATION, "operation clock exceeds document frontier")
			break
		}
		if !h.doc.Ready(op) {
			h.rejectOperation(p, collabv1.ErrorMessage_CODE_MISSING_DEPENDENCY, "missing dependency")
			break
		}
		status, err := h.doc.Apply(op)
		if err != nil {
			h.rejectOperation(p, collabv1.ErrorMessage_CODE_INVALID_OPERATION, "invalid operation")
			break
		}
		if status == crdt.Duplicate {
			continue
		}
		accepted = append(accepted, op)
	}
	return accepted
}

func (h *Hub) rejectOperation(p *Peer, code collabv1.ErrorMessage_Code, message string) {
	p.TrySend(EncodeError(code, message))
	p.CloseWithReason("rejected operation")
	h.dropPeer(p, true)
}

func (h *Hub) persistAndBroadcast(accepted []crdt.Op) bool {
	if len(accepted) == 0 {
		return false
	}
	ctx, cancel := context.WithTimeout(context.Background(), appendTimeout)
	err := h.store.Append(ctx, h.docID, accepted)
	cancel()
	if err != nil {
		h.failStop()
		return true
	}
	pb := make([]*crdtv1.Operation, len(accepted))
	for i, op := range accepted {
		pb[i] = crdt.OpToProto(op)
	}
	frame := mustMarshal(&collabv1.ServerMessage{
		Payload: &collabv1.ServerMessage_OperationBroadcast{OperationBroadcast: &collabv1.OperationBroadcast{
			Operations: pb,
		}},
	})
	h.broadcast(frame, 0, true)
	return false
}

func (h *Hub) handleCursor(ev cursorEvent) {
	if h.dead {
		return
	}
	p, ok := h.peers[ev.id]
	if !ok {
		return
	}
	if !h.doc.Has(ev.anchor) || !h.doc.Has(ev.head) {
		p.TrySend(EncodeError(collabv1.ErrorMessage_CODE_INVALID_OPERATION, "invalid operation"))
		return
	}
	cur := cursor{anchor: ev.anchor, head: ev.head}
	h.cursors[ev.id] = cur
	frame := mustMarshal(&collabv1.ServerMessage{
		Payload: &collabv1.ServerMessage_PresenceBroadcast{PresenceBroadcast: &collabv1.PresenceBroadcast{
			Client: uint64(ev.id),
			State:  &collabv1.PresenceBroadcast_Cursor{Cursor: cursorProto(cur)},
		}},
	})
	h.broadcast(frame, ev.id, false)
}

func (h *Hub) failStop() {
	frame := EncodeError(collabv1.ErrorMessage_CODE_INTERNAL, "internal error")
	for _, p := range h.peers {
		p.TrySend(frame)
		p.CloseWithReason("persistence failure")
	}
	h.peers = nil
	h.cursors = nil
	h.dead = true
	h.registry.evict(h)
}

func (h *Hub) drop(id crdt.ClientID, announce bool) {
	p, ok := h.peers[id]
	if !ok {
		return
	}
	h.dropPeer(p, announce)
}

func (h *Hub) dropPeer(p *Peer, announce bool) {
	id := p.id
	if h.peers[id] != p {
		return
	}
	delete(h.peers, id)
	delete(h.cursors, id)
	p.CloseWithReason("session ended")
	if announce && !h.dead {
		frame := mustMarshal(&collabv1.ServerMessage{
			Payload: &collabv1.ServerMessage_PresenceBroadcast{PresenceBroadcast: &collabv1.PresenceBroadcast{
				Client: uint64(id),
				State:  &collabv1.PresenceBroadcast_Left{Left: &collabv1.PresenceLeft{}},
			}},
		})
		h.broadcast(frame, id, false)
	}
}

func (h *Hub) sendOrDrop(p *Peer, frame []byte) bool {
	if p.TrySend(frame) {
		return true
	}
	p.CloseWithReason("slow consumer")
	h.drop(p.id, true)
	return false
}

func (h *Hub) broadcast(frame []byte, except crdt.ClientID, includeExcept bool) {
	var dead []*Peer
	for id, p := range h.peers {
		if !includeExcept && id == except {
			continue
		}
		if !p.TrySend(frame) {
			p.CloseWithReason("slow consumer")
			dead = append(dead, p)
		}
	}
	for _, p := range dead {
		h.dropPeer(p, true)
	}
}

func cursorProto(c cursor) *collabv1.CursorMove {
	return &collabv1.CursorMove{
		Anchor: &crdtv1.OpID{Client: uint64(c.anchor.Client), Clock: c.anchor.Clock},
		Head:   &crdtv1.OpID{Client: uint64(c.head.Client), Clock: c.head.Clock},
	}
}

func (h *Hub) send(ctx context.Context, ev event) error {
	select {
	case <-h.stopped:
		return ErrStopped
	default:
	}
	select {
	case h.events <- ev:
		return nil
	case <-h.stopped:
		return ErrStopped
	case <-ctx.Done():
		return ctx.Err()
	}
}

func waitReply[T any](ctx context.Context, h *Hub, reply <-chan T) (T, error) {
	var zero T
	select {
	case v := <-reply:
		return v, nil
	case <-h.stopped:
		return zero, ErrStopped
	case <-ctx.Done():
		return zero, ctx.Err()
	}
}
