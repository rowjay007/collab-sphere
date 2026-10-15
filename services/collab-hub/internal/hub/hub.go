package hub

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	collabv1 "github.com/rowjay007/collab-sphere/gen/collab/v1"
	crdtv1 "github.com/rowjay007/collab-sphere/gen/crdt/v1"
	"github.com/rowjay007/collab-sphere/pkg/crdt"
	"github.com/rowjay007/collab-sphere/services/collab-hub/internal/fanout"
)

const (
	appendTimeout     = 5 * time.Second
	loadTimeout       = 10 * time.Second
	readTimeout       = 5 * time.Second
	maxOpsPerGroup    = 4096
	defaultGapGrace   = 30 * time.Millisecond
	fillLimit         = 4096
	maxBufferedRanges = 64
	maxFillFailures   = 3
)

var (
	// ErrStopped is returned when a request lands on a hub that is shutting down.
	ErrStopped = errors.New("hub stopped")
	// ErrUnavailable is returned when a join cannot be served until the log is readable.
	ErrUnavailable = errors.New("document log unavailable")

	errDiverged   = errors.New("sequenced op does not apply")
	errNoProgress = errors.New("log read returned nothing new")
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

type timerEvent struct{ gen uint64 }

func (timerEvent) isEvent() {}

type stopEvent struct{}

func (stopEvent) isEvent() {}

type cursor struct {
	anchor crdt.ID
	head   crdt.ID
}

// gapTimer is the gap-healing state: at most one armed timer, tagged with a
// generation so a stale one is ignored, and the consecutive failed fills
// that set its backoff.
type gapTimer struct {
	gen      uint64
	armed    bool
	failures int
}

// Hub owns one document. Between events, doc holds log[1..D] plus the ops
// this hub appended that still wait above D in seq, where D is seq.Delivered().
type Hub struct {
	docID    string
	store    Store
	registry *Registry
	gapGrace time.Duration
	events   chan event
	stopped  chan struct{}
	once     sync.Once
	refs     int

	doc     *crdt.Doc
	seq     *fanout.Sequencer
	gap     gapTimer
	loadErr error
	peers   map[crdt.ClientID]*Peer
	cursors map[crdt.ClientID]cursor
	dead    bool
}

func newHub(docID string, registry *Registry) *Hub {
	return &Hub{
		docID:    docID,
		store:    registry.store,
		registry: registry,
		gapGrace: registry.gapGrace,
		events:   make(chan event, 256),
		stopped:  make(chan struct{}),
		peers:    make(map[crdt.ClientID]*Peer),
		cursors:  make(map[crdt.ClientID]cursor),
	}
}

func (h *Hub) loop() {
	ctx, cancel := context.WithTimeout(context.Background(), loadTimeout)
	doc, delivered, err := h.store.Load(ctx, h.docID)
	cancel()
	if err != nil {
		h.loadErr = err
	} else {
		h.doc = doc
		h.seq = fanout.NewSequencer(delivered)
	}

	var pending event
	for {
		var ev event
		if pending != nil {
			ev, pending = pending, nil
		} else {
			ev = <-h.events
		}
		var stop bool
		switch ev := ev.(type) {
		case stopEvent:
			stop = true
		case joinEvent:
			stop = h.handleJoin(ev)
		case leaveEvent:
			h.handleLeave(ev)
		case opsEvent:
			pending, stop = h.handleOpsGroup(ev)
		case cursorEvent:
			h.handleCursor(ev)
		case timerEvent:
			stop = h.handleTimer(ev)
		}
		if stop {
			h.shutdown()
			return
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

func (h *Hub) handleJoin(ev joinEvent) bool {
	if h.dead {
		ev.reply <- ErrStopped
		return false
	}
	if h.loadErr != nil {
		message := "failed to load document"
		if errors.Is(h.loadErr, ErrArchived) {
			message = "document archived"
		}
		ev.peer.TrySend(EncodeError(collabv1.ErrorMessage_CODE_INTERNAL, message))
		ev.reply <- h.loadErr
		return false
	}
	// A snapshot holding an op above D would let the joiner build on an op
	// another hub has not delivered, so the snapshot must be exactly log[1..D].
	if open, _ := h.seq.Gap(); open {
		if h.heal() {
			ev.reply <- ErrStopped
			return true
		}
		if open, _ := h.seq.Gap(); open {
			ev.peer.TrySend(EncodeError(collabv1.ErrorMessage_CODE_UNAVAILABLE, "document log unavailable"))
			ev.reply <- ErrUnavailable
			return false
		}
	}

	if old, ok := h.peers[ev.peer.id]; ok {
		delete(h.peers, old.id)
		old.CloseWithReason("superseded")
	}
	h.peers[ev.peer.id] = ev.peer
	sync := mustMarshal(&collabv1.ServerMessage{
		Payload: &collabv1.ServerMessage_DocumentSync{DocumentSync: &collabv1.DocumentSync{
			DocumentId: h.docID,
			Snapshot:   h.doc.Snapshot(),
			Seq:        h.seq.Delivered(),
		}},
	})
	if !h.sendOrDrop(ev.peer, sync) {
		ev.reply <- errors.New("send buffer full")
		return false
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
			return false
		}
	}
	ev.reply <- nil
	return false
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
				return next, h.persist(accepted)
			}
			if len(ev.ops) > maxOpsPerGroup-opCount {
				return ev, h.persist(accepted)
			}
			opCount += len(ev.ops)
			accepted = append(accepted, h.applyOps(ev)...)
		default:
			return nil, h.persist(accepted)
		}
	}
	return nil, h.persist(accepted)
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

// persist appends the group and offers every seq run to the Sequencer. A run
// can hold ops the log already had above D, which reveals a gap. It reports
// whether the hub fail-stopped.
func (h *Hub) persist(accepted []crdt.Op) bool {
	if len(accepted) == 0 {
		return false
	}
	ctx, cancel := context.WithTimeout(context.Background(), appendTimeout)
	res, err := h.store.Append(ctx, h.docID, accepted)
	cancel()
	if err != nil {
		h.failStop(collabv1.ErrorMessage_CODE_UNAVAILABLE, "document service unavailable")
		return true
	}
	for _, run := range seqRuns(accepted, res.Seqs) {
		if err := h.deliver(h.seq.Offer(run)); err != nil {
			h.failStop(collabv1.ErrorMessage_CODE_INTERNAL, "internal error")
			return true
		}
	}
	open, buffered := h.seq.Gap()
	switch {
	case !open:
		return false
	case buffered > maxBufferedRanges:
		return h.heal()
	}
	h.armGap()
	return false
}

func seqRuns(ops []crdt.Op, seqs []uint64) []fanout.Batch {
	var runs []fanout.Batch
	for i, op := range ops {
		if n := len(runs); n > 0 && runs[n-1].Next() == seqs[i] {
			runs[n-1].Ops = append(runs[n-1].Ops, op)
			continue
		}
		runs = append(runs, fanout.Batch{FirstSeq: seqs[i], Ops: []crdt.Op{op}})
	}
	return runs
}

// deliver applies each batch to doc and sends it to every peer. The hub's own
// ops are already in doc and come back Duplicate.
func (h *Hub) deliver(ready []fanout.Batch) error {
	for _, b := range ready {
		pb := make([]*crdtv1.Operation, len(b.Ops))
		for i, op := range b.Ops {
			if _, err := h.doc.Apply(op); err != nil {
				return fmt.Errorf("%w: seq %d: %v", errDiverged, b.FirstSeq+uint64(i), err)
			}
			pb[i] = crdt.OpToProto(op)
		}
		frame := mustMarshal(&collabv1.ServerMessage{
			Payload: &collabv1.ServerMessage_OperationBroadcast{OperationBroadcast: &collabv1.OperationBroadcast{
				Operations: pb,
				FirstSeq:   b.FirstSeq,
			}},
		})
		h.broadcast(frame, 0, true)
	}
	return nil
}

func (h *Hub) handleTimer(ev timerEvent) bool {
	if h.dead || !h.gap.armed || ev.gen != h.gap.gen {
		return false
	}
	h.gap.armed = false
	return h.heal()
}

func (h *Hub) armGap() {
	if h.gap.armed {
		return
	}
	h.gap.gen++
	h.gap.armed = true
	gen := h.gap.gen
	time.AfterFunc(h.gapGrace<<h.gap.failures, func() {
		_ = h.send(context.Background(), timerEvent{gen: gen})
	})
}

// heal fills the gap from the log and schedules a retry with backoff when it
// cannot. It reports whether the hub fail-stopped.
func (h *Hub) heal() bool {
	err := h.fill()
	switch {
	case err == nil:
		h.gap.failures = 0
		h.gap.armed = false
		return false
	case errors.Is(err, errDiverged):
		h.failStop(collabv1.ErrorMessage_CODE_INTERNAL, "internal error")
		return true
	}
	h.gap.failures++
	if h.gap.failures >= maxFillFailures {
		h.failStop(collabv1.ErrorMessage_CODE_UNAVAILABLE, "document log unavailable")
		return true
	}
	h.armGap()
	return false
}

func (h *Hub) fill() error {
	for {
		if open, _ := h.seq.Gap(); !open {
			return nil
		}
		ctx, cancel := context.WithTimeout(context.Background(), readTimeout)
		b, _, err := h.store.ReadOperations(ctx, h.docID, h.seq.Delivered(), fillLimit)
		cancel()
		if err != nil {
			return err
		}
		ready := h.seq.Offer(b)
		if len(ready) == 0 {
			return errNoProgress
		}
		if err := h.deliver(ready); err != nil {
			return err
		}
	}
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

func (h *Hub) failStop(code collabv1.ErrorMessage_Code, message string) {
	frame := EncodeError(code, message)
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
