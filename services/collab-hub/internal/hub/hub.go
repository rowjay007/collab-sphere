package hub

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"
	"sync"
	"time"

	collabv1 "github.com/rowjay007/collab-sphere/gen/collab/v1"
	crdtv1 "github.com/rowjay007/collab-sphere/gen/crdt/v1"
	presencev1 "github.com/rowjay007/collab-sphere/gen/presence/v1"
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
	defaultMaxReplay  = 20_000
	defaultIdleSync   = time.Second
	defaultReadyWait  = 2 * time.Second
	remoteInbox       = 1024
	presenceInbox     = 1024
	presenceTimeout   = 5 * time.Second
	joinMinBackoff    = 100 * time.Millisecond
	joinMaxBackoff    = 2 * time.Second
	// A frame of this many ops stays far below the client's read limit, and
	// defaultMaxReplay/missedFrameOps frames fit in a new peer's empty buffer.
	missedFrameOps = 4096
)

var (
	// ErrStopped is returned when a request lands on a hub that is shutting down.
	ErrStopped = errors.New("hub stopped")
	// ErrUnavailable is returned when a join cannot be served until the log is readable.
	ErrUnavailable = errors.New("document log unavailable")
	// ErrInvalidResume is returned when a client resumes from a seq the log never reached.
	ErrInvalidResume = errors.New("resume seq is beyond the document log")

	errDiverged   = errors.New("sequenced op does not apply")
	errNoProgress = errors.New("log read returned nothing new")
)

type event interface{ isEvent() }

type joinEvent struct {
	peer   *Peer
	resume bool
	seq    uint64
	reply  chan error
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
	peer   *Peer
	anchor crdt.ID
	head   crdt.ID
}

func (cursorEvent) isEvent() {}

type renameEvent struct {
	peer *Peer
	name string
}

func (renameEvent) isEvent() {}

type presenceEvent struct{ ev *presencev1.Event }

func (presenceEvent) isEvent() {}

type timerEvent struct{ gen uint64 }

func (timerEvent) isEvent() {}

type remoteOpsEvent struct{ b fanout.Batch }

func (remoteOpsEvent) isEvent() {}

type resyncEvent struct{}

func (resyncEvent) isEvent() {}

type idleEvent struct{}

func (idleEvent) isEvent() {}

type stopEvent struct{}

func (stopEvent) isEvent() {}

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
	pres     Presence
	registry *Registry
	gapGrace time.Duration
	events   chan event
	remote   chan fanout.Batch
	presence chan *presencev1.Event
	resync   chan struct{}
	stopped  chan struct{}
	once     sync.Once
	refs     int

	sub     fanout.Subscription
	doc     *crdt.Doc
	seq     *fanout.Sequencer
	gap     gapTimer
	loadErr error
	peers   map[crdt.ClientID]*Peer
	dead    bool
}

func newHub(docID string, registry *Registry) *Hub {
	return &Hub{
		docID:    docID,
		store:    registry.store,
		pres:     registry.pres,
		registry: registry,
		gapGrace: registry.gapGrace,
		events:   make(chan event, 256),
		remote:   make(chan fanout.Batch, remoteInbox),
		presence: make(chan *presencev1.Event, presenceInbox),
		resync:   make(chan struct{}, 1),
		stopped:  make(chan struct{}),
		peers:    make(map[crdt.ClientID]*Peer),
	}
}

func (h *Hub) loop() {
	h.subscribe()
	ctx, cancel := context.WithTimeout(context.Background(), loadTimeout)
	doc, delivered, err := h.store.Load(ctx, h.docID)
	cancel()
	if err != nil {
		h.loadErr = err
		h.unsubscribe()
	} else {
		h.doc = doc
		h.seq = fanout.NewSequencer(delivered)
	}

	idle := time.NewTimer(h.registry.idleSync)
	defer idle.Stop()
	var pending event
	for {
		ev := pending
		pending = nil
		if ev == nil {
			select {
			case ev = <-h.events:
			case b := <-h.remote:
				ev = remoteOpsEvent{b: b}
			case p := <-h.presence:
				ev = presenceEvent{ev: p}
			case <-h.resync:
				ev = resyncEvent{}
			case <-idle.C:
				ev = idleEvent{}
			}
		}
		before := h.delivered()
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
		case renameEvent:
			h.handleRename(ev)
		case presenceEvent:
			h.handlePresence(ev.ev)
		case timerEvent:
			stop = h.handleTimer(ev)
		case remoteOpsEvent:
			stop = h.handleRemote(ev.b)
		case resyncEvent:
			h.resyncPresence()
			stop = h.poll()
		case idleEvent:
			if len(h.peers) > 0 {
				stop = h.poll()
			}
			idle.Reset(h.registry.idleSync)
		}
		if stop {
			h.shutdown()
			return
		}
		if h.delivered() != before {
			idle.Reset(h.registry.idleSync)
		}
	}
}

// subscribe joins the bus before Load so no batch can fall between the two.
// If Redis does not confirm in time the hub starts degraded and resyncs once
// it does.
func (h *Hub) subscribe() {
	sub := h.registry.bus.Join(h.docID, h)
	h.sub = sub
	select {
	case <-sub.Ready():
	case <-time.After(h.registry.readyWait):
		go func() {
			select {
			case <-sub.Ready():
				h.Resync()
			case <-h.stopped:
			}
		}()
	}
}

func (h *Hub) unsubscribe() {
	if h.sub != nil {
		h.sub.Close()
		h.sub = nil
	}
}

// RemoteOps hands a batch from the bus to the loop. A full inbox drops it,
// and gap or idle healing recovers what it held.
func (h *Hub) RemoteOps(b fanout.Batch) {
	select {
	case h.remote <- b:
	default:
	}
}

// Presence hands a presence event to the loop. A full inbox drops it and
// asks for a Resync, whose AwarenessSync repairs every peer.
func (h *Hub) Presence(ev *presencev1.Event) {
	select {
	case h.presence <- ev:
	default:
		h.Resync()
	}
}

// Resync asks the loop to read the log and refresh every peer's presence.
// Repeated requests coalesce.
func (h *Hub) Resync() {
	select {
	case h.resync <- struct{}{}:
	default:
	}
}

func (h *Hub) delivered() uint64 {
	if h.seq == nil {
		return 0
	}
	return h.seq.Delivered()
}

func (h *Hub) closeStopped() {
	h.once.Do(func() { close(h.stopped) })
}

func (h *Hub) shutdown() {
	h.closeStopped()
	h.unsubscribe()
	h.dead = true
	frame := EncodeError(collabv1.ErrorMessage_CODE_UNAVAILABLE, "hub stopped")
	for _, p := range h.peers {
		p.TrySend(frame)
		p.CloseWithReason("hub stopped")
		h.leavePresence(p)
	}
	h.peers = nil
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
		if errors.Is(h.loadErr, ErrArchived) {
			ev.peer.TrySend(EncodeError(collabv1.ErrorMessage_CODE_INTERNAL, "document archived"))
		} else {
			ev.peer.TrySend(EncodeError(collabv1.ErrorMessage_CODE_UNAVAILABLE, "failed to load document"))
		}
		ev.reply <- h.loadErr
		return false
	}
	// A resuming client holds log[1..seq], so D must reach seq before its ops
	// are validated. A snapshot holding an op above D would let the joiner
	// build on an op another hub has not delivered, so G must be empty too.
	var target uint64
	if ev.resume {
		target = ev.seq
	}
	if open, _ := h.seq.Gap(); open || h.seq.Delivered() < target {
		err := h.heal(target)
		switch {
		case h.dead:
			ev.reply <- ErrStopped
			return true
		case errors.Is(err, ErrInvalidResume):
			ev.peer.TrySend(EncodeError(collabv1.ErrorMessage_CODE_INVALID_RESUME, ErrInvalidResume.Error()))
			ev.reply <- ErrInvalidResume
			return false
		case err != nil:
			ev.peer.TrySend(EncodeError(collabv1.ErrorMessage_CODE_UNAVAILABLE, "document log unavailable"))
			ev.reply <- ErrUnavailable
			return false
		}
	}
	frames, err := h.joinFrames(ev.resume, ev.seq)
	if err != nil {
		ev.peer.TrySend(EncodeError(collabv1.ErrorMessage_CODE_UNAVAILABLE, "document log unavailable"))
		ev.reply <- ErrUnavailable
		return false
	}

	// A superseded peer gets no presence Leave: the new session's Join
	// replaces its state, so nobody sees the client leave.
	var superseded <-chan struct{}
	if old, ok := h.peers[ev.peer.id]; ok {
		delete(h.peers, old.id)
		old.TrySend(EncodeError(collabv1.ErrorMessage_CODE_SUPERSEDED, "superseded by a newer connection"))
		old.CloseWithReason("superseded")
		superseded = old.joined
	}
	h.peers[ev.peer.id] = ev.peer
	for _, frame := range frames {
		if !h.sendOrDrop(ev.peer, frame) {
			ev.reply <- errors.New("send buffer full")
			return false
		}
	}
	h.joinPresence(ev.peer, superseded)
	ev.reply <- nil
	return false
}

// joinPresence runs the presence Join off the loop and sends the result as
// the peer's AwarenessSync. A failed Join sends an empty one and retries
// with backoff while the peer is attached, then sends the fresh one. It
// waits for a superseded peer's Join so the newer session is written last.
func (h *Hub) joinPresence(p *Peer, superseded <-chan struct{}) {
	joined := make(chan struct{})
	p.joined = joined
	sess := p.presence()
	h.registry.presenceCalls.Go(func() {
		defer close(joined)
		if superseded != nil {
			<-superseded
		}
		for backoff := joinMinBackoff; ; backoff = min(2*backoff, joinMaxBackoff) {
			select {
			case <-p.Done():
				return
			default:
			}
			ctx, cancel := context.WithTimeout(context.Background(), presenceTimeout)
			states, version, err := h.pres.Join(ctx, h.docID, sess)
			cancel()
			if err == nil {
				p.TrySend(encodeAwarenessSync(states, version, p.id))
				return
			}
			if backoff == joinMinBackoff {
				p.TrySend(encodeAwarenessSync(nil, 0, p.id))
			}
			select {
			case <-p.Done():
				return
			case <-time.After(backoff):
			}
		}
	})
}

// leavePresence removes the peer's session once its Join has returned, so a
// Leave never lands before the Join it undoes.
func (h *Hub) leavePresence(p *Peer) {
	joined := p.joined
	if joined == nil {
		return
	}
	sess := p.presence()
	select {
	case <-joined:
		h.pres.Leave(h.docID, sess)
	default:
		h.registry.presenceCalls.Go(func() {
			<-joined
			h.pres.Leave(h.docID, sess)
		})
	}
}

// resyncPresence sends every peer a fresh AwarenessSync after the bus may
// have lost presence events.
func (h *Hub) resyncPresence() {
	if h.dead || len(h.peers) == 0 {
		return
	}
	peers := slices.Collect(maps.Values(h.peers))
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), presenceTimeout)
		states, version, err := h.pres.List(ctx, h.docID)
		cancel()
		if err != nil {
			return
		}
		for _, p := range peers {
			p.TrySend(encodeAwarenessSync(states, version, p.id))
		}
	}()
}

// joinFrames answers a resume within maxReplay with MissedOps for
// log(seq, D] and anything else with a DocumentSync of log[1..D]. MissedOps
// is split into frames of at most missedFrameOps ops with contiguous
// first_seq, and every frame carries seq D so the client knows when it has
// them all. It is sent even when empty.
func (h *Hub) joinFrames(resume bool, seq uint64) ([][]byte, error) {
	d := h.seq.Delivered()
	if !resume || d-seq > h.registry.maxReplay {
		return [][]byte{mustMarshal(&collabv1.ServerMessage{
			Payload: &collabv1.ServerMessage_DocumentSync{DocumentSync: &collabv1.DocumentSync{
				DocumentId: h.docID,
				Snapshot:   h.doc.Snapshot(),
				Seq:        d,
			}},
		})}, nil
	}
	var frames [][]byte
	for after := seq; ; {
		var ops []*crdtv1.Operation
		if after < d {
			ctx, cancel := context.WithTimeout(context.Background(), readTimeout)
			b, _, err := h.store.ReadOperations(ctx, h.docID, after, int(min(d-after, missedFrameOps)))
			cancel()
			if err != nil {
				return nil, err
			}
			if len(b.Ops) == 0 {
				return nil, errNoProgress
			}
			ops = make([]*crdtv1.Operation, len(b.Ops))
			for i, op := range b.Ops {
				ops[i] = crdt.OpToProto(op)
			}
		}
		frames = append(frames, mustMarshal(&collabv1.ServerMessage{
			Payload: &collabv1.ServerMessage_MissedOps{MissedOps: &collabv1.MissedOps{
				DocumentId: h.docID,
				FirstSeq:   after + 1,
				Operations: ops,
				Seq:        d,
			}},
		}))
		after += uint64(len(ops))
		if after == d {
			return frames, nil
		}
	}
}

func (h *Hub) handleLeave(ev leaveEvent) {
	h.dropPeer(ev.peer)
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
	h.dropPeer(p)
}

// persist appends the group, publishes what the log newly stored, and offers
// every seq run to the Sequencer. A run can hold ops the log already had
// above D, which reveals a gap. It reports whether the hub fail-stopped.
func (h *Hub) persist(accepted []crdt.Op) bool {
	if len(accepted) == 0 {
		return false
	}
	ctx, cancel := context.WithTimeout(context.Background(), appendTimeout)
	res, err := h.store.Append(ctx, h.docID, accepted)
	cancel()
	if err != nil {
		var conflict ConflictError
		if errors.As(err, &conflict) {
			if p, ok := h.peers[conflict.Client]; ok {
				h.rejectOperation(p, collabv1.ErrorMessage_CODE_INVALID_OPERATION, "operation conflicts with the log")
			}
		}
		h.failStop(collabv1.ErrorMessage_CODE_UNAVAILABLE, "document service unavailable")
		return true
	}
	if b, ok := newlyStored(accepted, res); ok {
		h.registry.bus.Publish(h.docID, b)
	}
	for _, run := range seqRuns(accepted, res.Seqs) {
		if err := h.deliver(h.seq.Offer(run)); err != nil {
			h.failStop(collabv1.ErrorMessage_CODE_INTERNAL, "internal error")
			return true
		}
	}
	return h.watchGap()
}

func (h *Hub) handleRemote(b fanout.Batch) bool {
	if h.dead || h.seq == nil {
		return false
	}
	if err := h.deliver(h.seq.Offer(b)); err != nil {
		h.failStop(collabv1.ErrorMessage_CODE_INTERNAL, "internal error")
		return true
	}
	return h.watchGap()
}

// watchGap arms the grace timer for an open gap, or fills at once when too
// many ranges wait. It reports whether the hub fail-stopped.
func (h *Hub) watchGap() bool {
	open, buffered := h.seq.Gap()
	switch {
	case !open:
		return false
	case buffered > maxBufferedRanges:
		_ = h.heal(0)
		return h.dead
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

// newlyStored returns the ops Append stored for the first time, which hold
// exactly the seqs FirstNewSeq..OpSeq.
func newlyStored(ops []crdt.Op, res AppendResult) (fanout.Batch, bool) {
	if res.FirstNewSeq == 0 {
		return fanout.Batch{}, false
	}
	run := make([]crdt.Op, res.OpSeq-res.FirstNewSeq+1)
	for i, seq := range res.Seqs {
		if seq >= res.FirstNewSeq {
			run[seq-res.FirstNewSeq] = ops[i]
		}
	}
	return fanout.Batch{FirstSeq: res.FirstNewSeq, Ops: run}, true
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
	_ = h.heal(0)
	return h.dead
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

// heal fills from the log until no gap is open and D is at least target, and
// schedules a retry with backoff when it cannot. A failed read counts toward
// the fail-stop; a target beyond the log does not. h.dead reports whether
// the hub fail-stopped.
func (h *Hub) heal(target uint64) error {
	err := h.fill(target, false)
	switch {
	case err == nil:
		h.gap.failures = 0
		h.gap.armed = false
		return nil
	case errors.Is(err, ErrInvalidResume):
		return err
	case errors.Is(err, errDiverged):
		h.failStop(collabv1.ErrorMessage_CODE_INTERNAL, "internal error")
		return err
	}
	h.gap.failures++
	if h.gap.failures >= maxFillFailures {
		h.failStop(collabv1.ErrorMessage_CODE_UNAVAILABLE, "document log unavailable")
		return err
	}
	h.armGap()
	return err
}

// poll reads the log to its end. The idle and resync paths use it, so finding
// nothing is the normal outcome, and a failed read waits for the next idle
// tick instead of counting toward fail-stop. It reports whether the hub
// fail-stopped.
func (h *Hub) poll() bool {
	if h.dead || h.seq == nil {
		return false
	}
	if err := h.fill(0, true); errors.Is(err, errDiverged) {
		h.failStop(collabv1.ErrorMessage_CODE_INTERNAL, "internal error")
		return true
	}
	return false
}

// fill reads the log after D until no gap is open and D is at least target.
// With toHead it first learns the log's end and makes that the target.
func (h *Hub) fill(target uint64, toHead bool) error {
	for {
		if !toHead && h.caughtUp(target) {
			return nil
		}
		ctx, cancel := context.WithTimeout(context.Background(), readTimeout)
		b, opSeq, err := h.store.ReadOperations(ctx, h.docID, h.seq.Delivered(), fillLimit)
		cancel()
		if err != nil {
			return err
		}
		if opSeq < target {
			return ErrInvalidResume
		}
		if toHead {
			target, toHead = opSeq, false
		}
		ready := h.seq.Offer(b)
		if len(ready) == 0 {
			if h.caughtUp(target) {
				return nil
			}
			return errNoProgress
		}
		if err := h.deliver(ready); err != nil {
			return err
		}
	}
}

func (h *Hub) caughtUp(target uint64) bool {
	open, _ := h.seq.Gap()
	return !open && h.seq.Delivered() >= target
}

// handleCursor drops a cursor whose anchor or head the hub does not hold
// yet: it races the sender's in-flight ops and the next move replaces it.
func (h *Hub) handleCursor(ev cursorEvent) {
	if h.dead || h.peers[ev.peer.id] != ev.peer {
		return
	}
	if !h.doc.Has(ev.anchor) || !h.doc.Has(ev.head) {
		return
	}
	h.pres.Move(h.docID, ev.peer.presence(), ev.anchor, ev.head)
}

func (h *Hub) handleRename(ev renameEvent) {
	if h.dead || h.peers[ev.peer.id] != ev.peer {
		return
	}
	ev.peer.name = ev.name
	h.pres.Rename(h.docID, ev.peer.presence(), ev.name)
}

// handlePresence relays a presence event to every local peer except the
// client it is about.
func (h *Hub) handlePresence(ev *presencev1.Event) {
	if h.dead {
		return
	}
	if frame, ok := encodePresenceBroadcast(ev); ok {
		h.broadcast(frame, crdt.ClientID(ev.GetClient()), false)
	}
}

func (h *Hub) failStop(code collabv1.ErrorMessage_Code, message string) {
	frame := EncodeError(code, message)
	for _, p := range h.peers {
		p.TrySend(frame)
		p.CloseWithReason("persistence failure")
		h.leavePresence(p)
	}
	h.peers = nil
	h.dead = true
	h.unsubscribe()
	h.registry.evict(h)
}

func (h *Hub) dropPeer(p *Peer) {
	if h.peers[p.id] != p {
		return
	}
	delete(h.peers, p.id)
	p.CloseWithReason("session ended")
	h.leavePresence(p)
}

func (h *Hub) sendOrDrop(p *Peer, frame []byte) bool {
	if p.TrySend(frame) {
		return true
	}
	p.CloseWithReason("slow consumer")
	h.dropPeer(p)
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
		h.dropPeer(p)
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
