package hub

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	collabv1 "github.com/rowjay007/collab-sphere/gen/collab/v1"
	crdtv1 "github.com/rowjay007/collab-sphere/gen/crdt/v1"
	presencev1 "github.com/rowjay007/collab-sphere/gen/presence/v1"
	"github.com/rowjay007/collab-sphere/pkg/crdt"
	"github.com/rowjay007/collab-sphere/pkg/metrics"
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
	errNoGrant    = errors.New("no grant")
	errPolicyDown = errors.New("permission service unavailable")
	errSuperseded = errors.New("superseded")
	errPeerClosed = errors.New("peer closed")
)

const (
	policyPeriod   = 500 * time.Millisecond
	policyStale    = 2 * time.Second
	policyInFlight = 3
)

type event interface{ isEvent() }

// wakeEvent asks the loop to notice a policy stop. The poller sends it
// without blocking; the flag it set is what the loop acts on.
type wakeEvent struct{}

func (wakeEvent) isEvent() {}

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
	id           crdt.ClientID
	ops          []crdt.Op
	readUnixNano int64
}

func (opsEvent) isEvent() {}

type reportEvent struct {
	id     crdt.ClientID
	vector crdt.VersionVector
}

func (reportEvent) isEvent() {}

type reportDone struct{ id crdt.ClientID }

func (reportDone) isEvent() {}

// reportJob is one stamped vector waiting on or in the single RPC for its replica.
type reportJob struct {
	id      crdt.ClientID
	vector  crdt.VersionVector
	homeSeq uint64
	resync  bool
}

// reportSlot is the one in-flight report for a replica. The loop is the only writer.
type reportSlot struct {
	busy bool
	next *reportJob
}

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

// fillEvent is one ReadOperations result. The loop offers it. A gen that is
// not the in-flight read is discarded.
type fillEvent struct {
	gen       uint64
	after     uint64
	batch     fanout.Batch
	opSeq     uint64
	liveSeq   uint64
	collected crdt.VersionVector
	err       error
}

func (fillEvent) isEvent() {}

// joinReadyEvent is a MissedOps read finished off the loop. The loop sends
// the frames, then any live frames buffered while the read ran.
type joinReadyEvent struct {
	gen    uint64
	id     crdt.ClientID
	frames [][]byte
	err    error
}

func (joinReadyEvent) isEvent() {}

// fillReq is the one in-flight log read. after is Delivered at the start.
type fillReq struct {
	gen    uint64
	after  uint64
	target uint64
	toHead bool
	heal   bool
}

// fillNeed is the work that arrived while a read was in flight.
type fillNeed struct {
	target uint64
	toHead bool
	heal   bool
	set    bool
}

// opening is a peer whose MissedOps are not queued yet. Live frames go to
// buf and leave after those frames, which keeps ADR 0007 seq order.
type opening struct {
	ev  joinEvent
	gen uint64
	buf [][]byte
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
	reports map[crdt.ClientID]*reportSlot
	dead    bool

	filling *fillReq
	follow  fillNeed
	fillGen uint64
	catchup []joinEvent
	opening map[crdt.ClientID]*opening
	openGen uint64

	live          liveBuild
	appendThrough uint64

	// policy is published by the poller. The loop only loads it.
	// peerView is published by the loop. The poller only loads it.
	policy     atomic.Pointer[policyView]
	peerView   atomic.Pointer[peerSnap]
	stopApply  atomic.Bool
	attempted  atomic.Bool
	noteMu     sync.Mutex
	policyNote chan struct{}
}

func newHub(docID string, registry *Registry) *Hub {
	h := &Hub{
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
		reports:  make(map[crdt.ClientID]*reportSlot),
		opening:  make(map[crdt.ClientID]*opening),
	}
	if registry.Policy() != nil {
		h.policyNote = make(chan struct{})
	}
	return h
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
	if h.registry.Policy() != nil {
		h.waitFirstPolicy()
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
		if h.stopApply.Load() && !h.dead {
			if ev != nil {
				h.reject(ev)
			}
			h.failStop(collabv1.ErrorMessage_CODE_UNAVAILABLE, "permission service unavailable")
			ev = nil
		}
		h.reapClosed()
		before := h.delivered()
		var stop bool
		if !h.dead {
			switch ev := ev.(type) {
			case stopEvent:
				stop = true
			case joinEvent:
				stop = h.handleJoin(ev)
			case leaveEvent:
				h.handleLeave(ev)
			case opsEvent:
				pending, stop = h.handleOpsGroup(ev)
			case reportEvent:
				h.clientReported(ev.id)
				h.stampReport(ev.id, ev.vector, false)
			case reportDone:
				h.finishReport(ev.id)
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
			case fillEvent:
				stop = h.handleFill(ev)
			case liveBuildEvent:
				stop = h.handleLiveBuild(ev)
			case joinReadyEvent:
				stop = h.handleJoinReady(ev)
			}
		}
		if h.dead {
			stop = true
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
	h.cancelJoins(ErrStopped)
	frame := EncodeError(collabv1.ErrorMessage_CODE_UNAVAILABLE, "hub stopped")
	for _, p := range h.peers {
		p.TrySend(frame)
		p.CloseWithReason("hub stopped")
		h.leavePresence(p)
	}
	h.peers = nil
	h.publishPeers()
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
		replyJoin(ev, ErrStopped)
		return false
	}
	// admitFresh already ran on the caller's goroutine. A stale pointer still refuses.
	if err := h.admit(ev.peer); err != nil {
		h.refuseJoin(ev, err)
		return false
	}
	if h.loadErr != nil {
		if errors.Is(h.loadErr, ErrArchived) {
			ev.peer.TrySend(EncodeError(collabv1.ErrorMessage_CODE_INTERNAL, "document archived"))
		} else {
			ev.peer.TrySend(EncodeError(collabv1.ErrorMessage_CODE_UNAVAILABLE, "failed to load document"))
		}
		replyJoin(ev, h.loadErr)
		return false
	}
	h.takeOver(ev.peer.id)
	return h.progressJoin(ev)
}

// progressJoin serves the join once D covers a resume and no gap is open.
// Otherwise it parks on the single in-flight fill.
func (h *Hub) progressJoin(ev joinEvent) bool {
	// A resuming client holds log[1..seq], so D must reach seq before its ops
	// are validated. A snapshot holding an op above D would let the joiner
	// build on an op another hub has not delivered, so G must be empty too.
	var target uint64
	if ev.resume {
		target = ev.seq
	}
	if open, _ := h.seq.Gap(); open || h.seq.Delivered() < target {
		h.catchup = append(h.catchup, ev)
		h.requestFill(target, false, true)
		return false
	}
	h.serveJoin(ev)
	return h.dead
}

func (h *Hub) serveJoin(ev joinEvent) {
	d := h.seq.Delivered()
	if ev.resume && d < ev.seq {
		h.refuseResume(ev, ErrInvalidResume)
		return
	}
	forceSync := ev.peer.ResyncPending()
	if forceSync || !ev.resume || d-ev.seq > h.registry.maxReplay {
		frame := mustMarshal(&collabv1.ServerMessage{
			Payload: &collabv1.ServerMessage_DocumentSync{DocumentSync: &collabv1.DocumentSync{
				DocumentId: h.docID,
				Snapshot:   h.doc.Snapshot(),
				Seq:        d,
				Client:     uint64(ev.peer.id),
				Resync:     forceSync,
			}},
		})
		h.attachJoin(ev, [][]byte{frame}, nil)
		return
	}
	h.openGen++
	gen := h.openGen
	h.opening[ev.peer.id] = &opening{ev: ev, gen: gen}
	go h.readMissed(gen, ev.peer.id, ev.seq, d)
}

// readMissed loads MissedOps off the loop. The loop sends them on joinReady,
// before any live frame buffered for this peer.
func (h *Hub) readMissed(gen uint64, id crdt.ClientID, seq, d uint64) {
	frames, err := h.missedFrames(seq, d)
	_ = h.send(context.Background(), joinReadyEvent{gen: gen, id: id, frames: frames, err: err})
}

func (h *Hub) handleJoinReady(ev joinReadyEvent) bool {
	op := h.opening[ev.id]
	if op == nil || op.gen != ev.gen {
		return false
	}
	delete(h.opening, ev.id)
	if h.dead {
		replyJoin(op.ev, ErrStopped)
		return false
	}
	select {
	case <-op.ev.peer.Done():
		replyJoin(op.ev, errPeerClosed)
		return false
	default:
	}
	if ev.err != nil {
		op.ev.peer.TrySend(EncodeError(collabv1.ErrorMessage_CODE_UNAVAILABLE, "document log unavailable"))
		replyJoin(op.ev, ErrUnavailable)
		return false
	}
	h.attachJoin(op.ev, ev.frames, op.buf)
	return h.dead
}

func (h *Hub) attachJoin(ev joinEvent, frames, extra [][]byte) {
	if err := h.admit(ev.peer); err != nil {
		h.refuseJoin(ev, err)
		return
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
	h.publishPeers()
	for _, frame := range frames {
		if !h.sendJoinFrame(ev, frame) {
			return
		}
	}
	for _, frame := range extra {
		if !h.sendJoinFrame(ev, frame) {
			return
		}
	}
	resyncJoin := ev.peer.ResyncPending()
	if !ev.resume || resyncJoin {
		h.stampReport(ev.peer.id, h.doc.Vector(), resyncJoin)
	}
	h.joinPresence(ev.peer, superseded)
	replyJoin(ev, nil)
}

func (h *Hub) sendJoinFrame(ev joinEvent, frame []byte) bool {
	if !h.peerVisible(ev.peer) {
		ev.peer.TrySend(EncodeError(collabv1.ErrorMessage_CODE_REVOKED, "access revoked"))
		h.dropPeer(ev.peer)
		replyJoin(ev, errNoGrant)
		return false
	}
	if !h.sendOrDrop(ev.peer, frame) {
		replyJoin(ev, errors.New("send buffer full"))
		return false
	}
	return true
}

func (h *Hub) takeOver(id crdt.ClientID) {
	if op := h.opening[id]; op != nil {
		delete(h.opening, id)
		h.supersedeJoin(op.ev)
	}
	kept := h.catchup[:0]
	for _, ev := range h.catchup {
		if ev.peer.id == id {
			h.supersedeJoin(ev)
			continue
		}
		kept = append(kept, ev)
	}
	h.catchup = kept
}

func (h *Hub) supersedeJoin(ev joinEvent) {
	ev.peer.TrySend(EncodeError(collabv1.ErrorMessage_CODE_SUPERSEDED, "superseded by a newer connection"))
	ev.peer.CloseWithReason("superseded")
	replyJoin(ev, errSuperseded)
}

func (h *Hub) dropWaiting(p *Peer) {
	if op := h.opening[p.id]; op != nil && op.ev.peer == p {
		delete(h.opening, p.id)
		replyJoin(op.ev, errPeerClosed)
	}
	kept := h.catchup[:0]
	for _, ev := range h.catchup {
		if ev.peer == p {
			replyJoin(ev, errPeerClosed)
			continue
		}
		kept = append(kept, ev)
	}
	h.catchup = kept
}

func (h *Hub) cancelJoins(err error) {
	for _, ev := range h.catchup {
		h.refuseJoin(ev, err)
	}
	h.catchup = nil
	for id, op := range h.opening {
		h.refuseJoin(op.ev, err)
		delete(h.opening, id)
	}
}

func (h *Hub) refuseJoin(ev joinEvent, err error) {
	h.deny(ev.peer, err)
	replyJoin(ev, err)
}

func (h *Hub) refuseResume(ev joinEvent, err error) {
	ev.peer.TrySend(EncodeError(collabv1.ErrorMessage_CODE_INVALID_RESUME, err.Error()))
	replyJoin(ev, err)
}

func replyJoin(ev joinEvent, err error) {
	select {
	case ev.reply <- err:
	default:
	}
}

func (h *Hub) reapClosed() {
	for _, p := range h.peers {
		select {
		case <-p.Done():
			h.dropPeer(p)
		default:
		}
	}
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
			if !h.peerVisible(p) {
				continue
			}
			p.TrySend(encodeAwarenessSync(states, version, p.id))
		}
	}()
}

// missedFrames reads log(seq, d] into MissedOps frames of at most
// missedFrameOps ops. Every frame carries seq d. An empty frame is sent
// when seq is already d, so the client knows it is attached.
func (h *Hub) missedFrames(seq, d uint64) ([][]byte, error) {
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
			if uint64(len(b.Ops)) > d-after {
				b.Ops = b.Ops[:d-after]
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
	h.dropWaiting(ev.peer)
	h.dropPeer(ev.peer)
	close(ev.reply)
}

type reportMark struct {
	id     crdt.ClientID
	vector crdt.VersionVector
	clip   crdt.VersionVector
	after  int
}

// clientReported ends a pending resync: the socket is FIFO, so a report
// after the resync DocumentSync means the client applied that snapshot.
func (h *Hub) clientReported(id crdt.ClientID) {
	if p := h.peers[id]; p != nil {
		p.ClearResyncPending()
	}
}

func (h *Hub) markReport(ev reportEvent, after int) reportMark {
	h.clientReported(ev.id)
	clip := crdt.VersionVector{}
	if h.doc != nil {
		clip = h.doc.Vector()
	}
	return reportMark{id: ev.id, vector: ev.vector, clip: clip, after: after}
}

func (h *Hub) stampMarks(before uint64, seqs []uint64, marks []reportMark) {
	for _, mark := range marks {
		if h.doc == nil || h.dead {
			return
		}
		home := before
		if mark.after > 0 && mark.after <= len(seqs) {
			home = seqs[mark.after-1]
		}
		h.enqueueReport(&reportJob{
			id:      mark.id,
			vector:  clipVector(mark.vector, mark.clip),
			homeSeq: home,
		})
	}
}

func (h *Hub) stampReport(id crdt.ClientID, vector crdt.VersionVector, resync bool) {
	if h.doc == nil || h.dead {
		return
	}
	h.enqueueReport(&reportJob{
		id:      id,
		vector:  clipVector(vector, h.doc.Vector()),
		homeSeq: h.delivered(),
		resync:  resync,
	})
}

func clipVector(client, stored crdt.VersionVector) crdt.VersionVector {
	out := make(crdt.VersionVector)
	for id, clock := range client {
		have := stored[id]
		if have == 0 || clock == 0 {
			continue
		}
		out[id] = min(clock, have)
	}
	return out
}

func (h *Hub) enqueueReport(job *reportJob) {
	slot := h.reports[job.id]
	if slot == nil {
		slot = &reportSlot{}
		h.reports[job.id] = slot
	}
	if slot.busy {
		slot.next = job
		return
	}
	slot.busy = true
	go h.sendReport(job)
}

func (h *Hub) sendReport(job *reportJob) {
	ctx, cancel := context.WithTimeout(context.Background(), appendTimeout)
	err := h.store.ReportVector(ctx, h.docID, job.id, job.vector, job.homeSeq, job.resync)
	cancel()
	if errors.Is(err, ErrStaleReport) {
		slog.Debug("stale replica report", "doc", h.docID, "replica", job.id)
	} else if err != nil && !errors.Is(err, context.Canceled) {
		slog.Warn("replica report", "doc", h.docID, "replica", job.id, "err", err)
	}
	_ = h.send(context.Background(), reportDone{id: job.id})
}

func (h *Hub) finishReport(id crdt.ClientID) {
	slot := h.reports[id]
	if slot == nil {
		return
	}
	slot.busy = false
	next := slot.next
	slot.next = nil
	if next == nil || h.dead {
		h.freeReport(id)
		return
	}
	slot.busy = true
	go h.sendReport(next)
}

// freeReport drops an idle slot once its replica has no peer on this hub.
func (h *Hub) freeReport(id crdt.ClientID) {
	if slot := h.reports[id]; slot != nil && !slot.busy && h.peers[id] == nil {
		delete(h.reports, id)
	}
}

func (h *Hub) handleOpsGroup(first opsEvent) (event, bool) {
	start := time.Now()
	accepted := h.applyOps(first)
	readAt := int64(0)
	if len(accepted) > 0 {
		readAt = first.readUnixNano
	}
	opCount := len(first.ops)
	var marks []reportMark
	for opCount < maxOpsPerGroup {
		select {
		case next := <-h.events:
			switch ev := next.(type) {
			case opsEvent:
				if len(ev.ops) > maxOpsPerGroup-opCount {
					return ev, h.finishOps(start, accepted, readAt, marks)
				}
				opCount += len(ev.ops)
				more := h.applyOps(ev)
				if readAt == 0 && len(more) > 0 {
					readAt = ev.readUnixNano
				}
				accepted = append(accepted, more...)
			case reportEvent:
				marks = append(marks, h.markReport(ev, len(accepted)))
			default:
				return next, h.finishOps(start, accepted, readAt, marks)
			}
		default:
			return nil, h.finishOps(start, accepted, readAt, marks)
		}
	}
	return nil, h.finishOps(start, accepted, readAt, marks)
}

func (h *Hub) finishOps(start time.Time, accepted []crdt.Op, readAt int64, marks []reportMark) bool {
	before := h.delivered()
	seqs, stop := h.persist(accepted, readAt)
	if len(accepted) > 0 && h.registry.met != nil {
		metrics.ObserveSince(h.registry.met.Apply, start)
	}
	if !stop {
		h.stampMarks(before, seqs, marks)
	}
	return stop
}

func (h *Hub) opProblem(op crdt.Op) string {
	ins, ok := asInsert(op)
	if !ok {
		return ""
	}
	mode := h.doc.Mode()
	if ins.Cipher != nil && ins.Value != 0 {
		return "operation does not match document mode"
	}
	if mode == crdt.Encrypted {
		if ins.Cipher == nil {
			return "operation does not match document mode"
		}
		if h.registry.Policy() != nil && !ins.Copy && ins.Cipher.Epoch != h.currentEpoch() {
			return "epoch is not the open write epoch"
		}
		n := len(ins.Cipher.Bytes)
		if n < 17 || n > 20 {
			return "cipher length out of range"
		}
		if ins.Copy && !h.doc.CopyConsistent(ins) {
			return "copy value differs from its parent"
		}
		return ""
	}
	if ins.Cipher != nil {
		return "operation does not match document mode"
	}
	return ""
}

func asInsert(op crdt.Op) (crdt.Insert, bool) {
	switch op := op.(type) {
	case crdt.Insert:
		return op, true
	case *crdt.Insert:
		if op == nil {
			return crdt.Insert{}, false
		}
		return *op, true
	default:
		return crdt.Insert{}, false
	}
}

func (h *Hub) applyOps(ev opsEvent) []crdt.Op {
	if h.dead {
		return nil
	}
	p, ok := h.peers[ev.id]
	if !ok {
		return nil
	}
	if p.ResyncPending() {
		p.TrySend(EncodeError(collabv1.ErrorMessage_CODE_RESYNC, "replica resync required"))
		p.CloseWithReason("resync")
		h.dropPeer(p)
		return nil
	}
	if !h.gateWrite(p) {
		return nil
	}
	if h.registry.Policy() != nil && h.doc.Mode() == crdt.Encrypted && !h.epochOpen() {
		p.TrySend(EncodeError(collabv1.ErrorMessage_CODE_KEY_ROTATION, "key rotation"))
		return nil
	}
	accepted := make([]crdt.Op, 0, len(ev.ops))
	for _, op := range ev.ops {
		if msg := h.opProblem(op); msg != "" {
			h.rejectOperation(p, collabv1.ErrorMessage_CODE_INVALID_OPERATION, msg)
			break
		}
		if op.OpID().Client != ev.id {
			h.rejectOperation(p, collabv1.ErrorMessage_CODE_CLIENT_MISMATCH, "operation client does not match connection")
			break
		}
		if insert, ok := op.(crdt.Insert); ok && insert.Copy {
			h.rejectOperation(p, collabv1.ErrorMessage_CODE_INVALID_OPERATION, "copy inserts are server-only")
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
func (h *Hub) persist(accepted []crdt.Op, readAt int64) ([]uint64, bool) {
	if len(accepted) == 0 {
		return nil, false
	}
	ctx, cancel := context.WithTimeout(context.Background(), appendTimeout)
	appendStart := time.Now()
	res, err := h.store.Append(ctx, h.docID, accepted)
	cancel()
	if h.registry.met != nil {
		metrics.ObserveSince(h.registry.met.Append, appendStart)
	}
	if h.stopApply.Load() {
		h.failStop(collabv1.ErrorMessage_CODE_UNAVAILABLE, "permission service unavailable")
		return nil, true
	}
	if err != nil {
		var conflict ConflictError
		if errors.As(err, &conflict) {
			if p, ok := h.peers[conflict.Client]; ok {
				h.rejectOperation(p, collabv1.ErrorMessage_CODE_INVALID_OPERATION, "operation conflicts with the log")
			}
		}
		h.failStop(collabv1.ErrorMessage_CODE_UNAVAILABLE, "document service unavailable")
		return nil, true
	}
	if res.FirstNewSeq > 0 {
		h.appendThrough = res.OpSeq
	}
	if b, ok := newlyStored(accepted, res); ok {
		b.ReadUnixNano = readAt
		b.OriginUnixNano = time.Now().UnixNano()
		h.registry.bus.Publish(h.docID, b)
	}
	for _, run := range seqRuns(accepted, res.Seqs) {
		run.ReadUnixNano = readAt
		if err := h.deliver(h.seq.Offer(run)); err != nil {
			h.failStop(collabv1.ErrorMessage_CODE_INTERNAL, "internal error")
			return nil, true
		}
	}
	return res.Seqs, h.watchGap()
}

func (h *Hub) handleRemote(b fanout.Batch) bool {
	if h.dead || h.seq == nil {
		return false
	}
	if h.registry.met != nil {
		metrics.ObserveUnix(h.registry.met.FanoutLag, b.OriginUnixNano)
	}
	if err := h.deliver(h.seq.Offer(b)); err != nil {
		h.failStop(collabv1.ErrorMessage_CODE_INTERNAL, "internal error")
		return true
	}
	return h.watchGap()
}

// watchGap arms the grace timer for an open gap, or starts a fill at once
// when too many ranges wait.
func (h *Hub) watchGap() bool {
	open, buffered := h.seq.Gap()
	switch {
	case !open:
		return false
	case buffered > maxBufferedRanges:
		h.requestFill(0, false, true)
		return false
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
				Operations:   pb,
				FirstSeq:     b.FirstSeq,
				ReadUnixNano: b.ReadUnixNano,
			}},
		})
		h.broadcast(frame, 0, true, b.ReadUnixNano)
	}
	if h.appendThrough > 0 && h.delivered() >= h.appendThrough {
		h.appendThrough = 0
	}
	return nil
}

func (h *Hub) handleTimer(ev timerEvent) bool {
	if h.dead || !h.gap.armed || ev.gen != h.gap.gen {
		return false
	}
	h.gap.armed = false
	h.requestFill(0, false, true)
	return false
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

// poll asks for a read to the log's end. The idle and resync paths use it.
// A failed read waits for the next idle tick instead of counting toward
// fail-stop. Divergence is handled when the result arrives.
func (h *Hub) poll() bool {
	if h.dead || h.seq == nil {
		return false
	}
	h.requestFill(0, true, false)
	return false
}

// requestFill keeps one ReadOperations in flight. A result whose gen is not
// that read is discarded. Ops the loop already delivered are trimmed by Offer.
func (h *Hub) requestFill(target uint64, toHead, heal bool) {
	if h.dead || h.seq == nil {
		return
	}
	if h.filling != nil {
		if target > h.follow.target {
			h.follow.target = target
		}
		if toHead {
			h.follow.toHead = true
		}
		if heal {
			h.follow.heal = true
		}
		h.follow.set = true
		return
	}
	if !toHead && h.caughtUp(target) {
		return
	}
	h.startFill(target, toHead, heal)
}

func (h *Hub) startFill(target uint64, toHead, heal bool) {
	h.fillGen++
	after := h.seq.Delivered()
	gen := h.fillGen
	h.filling = &fillReq{gen: gen, after: after, target: target, toHead: toHead, heal: heal}
	docID := h.docID
	store := h.store
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), readTimeout)
		b, meta, err := store.ReadOperations(ctx, docID, after, fillLimit)
		cancel()
		_ = h.send(context.Background(), fillEvent{
			gen: gen, after: after, batch: b, opSeq: meta.OpSeq,
			liveSeq: meta.LiveSeq, collected: meta.Collected, err: err,
		})
	}()
}

func (h *Hub) handleFill(ev fillEvent) bool {
	req := h.filling
	if req == nil || ev.gen != req.gen {
		return false
	}
	follow := h.follow
	h.filling = nil
	h.follow = fillNeed{}
	if h.dead {
		return false
	}

	heal := req.heal || follow.heal
	target := max(req.target, follow.target)
	sawHead := req.toHead || follow.toHead
	if ev.err != nil {
		return h.finishFillErr(heal, ev.err)
	}
	h.maybeStartLiveBuild(ev.collected)
	if ev.opSeq < target && h.delivered() < target {
		h.refuseBeyond(ev.opSeq)
		target = ev.opSeq
	}
	if sawHead && ev.opSeq > target {
		target = ev.opSeq
	}
	// A read started at ev.after. Ops the loop delivered since then are dropped.
	cursor := h.delivered()
	batch := ev.batch
	if ev.after != cursor {
		batch = trimDelivered(batch, cursor)
	}
	ready := h.seq.Offer(batch)
	if len(ready) == 0 && !h.caughtUp(target) {
		// Delivery moved past the seq this read started at. The result cannot
		// close the gap that is open now, and it is not a failed read.
		if ev.after != cursor {
			h.armGap()
			return false
		}
		return h.finishFillErr(heal, errNoProgress)
	}
	if err := h.deliver(ready); err != nil {
		h.failStop(collabv1.ErrorMessage_CODE_INTERNAL, "internal error")
		h.cancelJoins(ErrStopped)
		return true
	}
	if !h.caughtUp(target) {
		h.startFill(target, false, heal)
		return false
	}
	h.gap.failures = 0
	h.gap.armed = false
	h.serveReadyJoins()
	return h.dead
}

// finishFillErr counts a heal failure toward fail-stop and answers parked
// joins now. A resume past the log is not a failure. Idle reads are not either.
func (h *Hub) finishFillErr(heal bool, err error) bool {
	if errors.Is(err, errDiverged) {
		h.failStop(collabv1.ErrorMessage_CODE_INTERNAL, "internal error")
		h.cancelJoins(ErrStopped)
		return true
	}
	if heal && !errors.Is(err, ErrInvalidResume) {
		h.gap.failures++
		if h.gap.failures >= maxFillFailures {
			h.failStop(collabv1.ErrorMessage_CODE_UNAVAILABLE, "document log unavailable")
			h.cancelJoins(ErrStopped)
			return true
		}
		h.armGap()
	}
	h.refuseCatchup(ErrUnavailable)
	return h.dead
}

func (h *Hub) refuseBeyond(opSeq uint64) {
	kept := h.catchup[:0]
	for _, ev := range h.catchup {
		if ev.resume && ev.seq > opSeq {
			h.refuseResume(ev, ErrInvalidResume)
			continue
		}
		kept = append(kept, ev)
	}
	h.catchup = kept
}

func (h *Hub) refuseCatchup(err error) {
	for _, ev := range h.catchup {
		h.refuseJoin(ev, err)
	}
	h.catchup = nil
}

func (h *Hub) serveReadyJoins() {
	pending := h.catchup
	h.catchup = nil
	var again []joinEvent
	for _, ev := range pending {
		if h.dead {
			replyJoin(ev, ErrStopped)
			continue
		}
		var target uint64
		if ev.resume {
			target = ev.seq
		}
		if open, _ := h.seq.Gap(); open || h.seq.Delivered() < target {
			again = append(again, ev)
			continue
		}
		h.serveJoin(ev)
	}
	if len(again) == 0 {
		return
	}
	h.catchup = append(h.catchup, again...)
	var target uint64
	for _, ev := range again {
		if ev.resume && ev.seq > target {
			target = ev.seq
		}
	}
	h.requestFill(target, false, true)
}

func trimDelivered(b fanout.Batch, delivered uint64) fanout.Batch {
	if len(b.Ops) == 0 || b.Next() <= delivered+1 {
		return fanout.Batch{}
	}
	next := delivered + 1
	if b.FirstSeq >= next {
		return b
	}
	return fanout.Batch{
		FirstSeq:       next,
		Ops:            b.Ops[next-b.FirstSeq:],
		OriginUnixNano: b.OriginUnixNano,
		ReadUnixNano:   b.ReadUnixNano,
	}
}

func (h *Hub) caughtUp(target uint64) bool {
	open, _ := h.seq.Gap()
	return !open && h.seq.Delivered() >= target
}

// handleCursor drops a cursor whose anchor or head the hub does not hold
// yet: it races the sender's in-flight ops and the next move replaces it.
func (h *Hub) handleCursor(ev cursorEvent) {
	if h.dead || h.peers[ev.peer.id] != ev.peer || !h.gatePresence(ev.peer) {
		return
	}
	if !h.doc.Has(ev.anchor) || !h.doc.Has(ev.head) {
		return
	}
	h.pres.Move(h.docID, ev.peer.presence(), ev.anchor, ev.head)
}

func (h *Hub) handleRename(ev renameEvent) {
	if h.dead || h.peers[ev.peer.id] != ev.peer || !h.gatePresence(ev.peer) {
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
		h.broadcast(frame, crdt.ClientID(ev.GetClient()), false, 0)
	}
}

func (h *Hub) failStop(code collabv1.ErrorMessage_Code, message string) {
	h.cancelJoins(ErrStopped)
	frame := EncodeError(code, message)
	for _, p := range h.peers {
		p.TrySend(frame)
		p.CloseWithReason("persistence failure")
		h.leavePresence(p)
	}
	h.peers = nil
	h.publishPeers()
	h.dead = true
	h.unsubscribe()
	h.registry.evict(h)
}

func (h *Hub) dropPeer(p *Peer) {
	if h.peers[p.id] != p {
		return
	}
	delete(h.peers, p.id)
	h.publishPeers()
	p.CloseWithReason("session ended")
	h.leavePresence(p)
	h.freeReport(p.id)
}

func (h *Hub) sendOrDrop(p *Peer, frame []byte) bool {
	if p.TrySend(frame) {
		return true
	}
	p.CloseWithReason("slow consumer")
	h.dropPeer(p)
	return false
}

func (h *Hub) broadcast(frame []byte, except crdt.ClientID, includeExcept bool, readUnixNano int64) {
	var dead []*Peer
	for id, p := range h.peers {
		if !includeExcept && id == except {
			continue
		}
		select {
		case <-p.Done():
			dead = append(dead, p)
			continue
		default:
		}
		if !h.peerVisible(p) {
			continue
		}
		if !p.TrySend(frame) {
			p.CloseWithReason("slow consumer")
			dead = append(dead, p)
			continue
		}
		if h.registry.met != nil {
			metrics.ObserveUnix(h.registry.met.Deliver, readUnixNano)
		}
	}
	for id, op := range h.opening {
		if !includeExcept && id == except {
			continue
		}
		op.buf = append(op.buf, frame)
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
