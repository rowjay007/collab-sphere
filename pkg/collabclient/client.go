// Package collabclient is a Go client for the collab-hub WebSocket protocol.
// It keeps a local CRDT replica, applies local edits optimistically, and
// applies every broadcast from the hub. Edits made while disconnected queue
// until the client attaches again and resumes from the last seq it holds.
package collabclient

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"math/rand/v2"
	"net/url"
	"slices"
	"strconv"
	"sync"
	"time"

	"github.com/coder/websocket"
	"google.golang.org/protobuf/proto"

	collabv1 "github.com/rowjay007/collab-sphere/gen/collab/v1"
	crdtv1 "github.com/rowjay007/collab-sphere/gen/crdt/v1"
	"github.com/rowjay007/collab-sphere/pkg/crdt"
)

const (
	maxOpsPerBatch   = 1024
	readLimit        = 64 << 20
	handshakeTimeout = 10 * time.Second
	minBackoff       = 50 * time.Millisecond
	maxBackoff       = 2 * time.Second
)

var (
	errClosed       = errors.New("collabclient: connection closed")
	errDisconnected = errors.New("collabclient: disconnected")
	errOffline      = errors.New("collabclient: not attached")
	errSuperseded   = errors.New("collabclient: superseded by a newer connection")
	// A broken stream is a hub bug that reconnecting cannot fix.
	errBrokenStream = errors.New("collabclient: broken stream")
)

// JoinKind is how the hub answered the last join.
type JoinKind int

const (
	// Sync means the hub sent a DocumentSync snapshot.
	Sync JoinKind = iota
	// Missed means the hub resumed the stream with MissedOps.
	Missed
)

// Options configures DialWith.
type Options struct {
	// URLs are hub base URLs (ws:// or wss://). DialWith tries them in order
	// and auto-reconnect rotates through them.
	URLs []string
	// AutoReconnect reconnects in the background when the connection ends
	// for any reason other than Close or Disconnect.
	AutoReconnect bool
}

type state int

const (
	offline    state = iota // no socket and no reconnect in progress
	connecting              // dialing or reading the join frames
	attached
	failed // closed, refused resume, or broken stream
)

// link is one WebSocket connection. Its goroutines act only while the
// client's generation still equals gen.
type link struct {
	gen    uint64
	conn   *websocket.Conn
	out    chan []byte
	ctx    context.Context
	cancel context.CancelFunc
	done   chan struct{}
}

// Client holds replica = log[1..seq] plus unacked, the ops this client made
// that it has not yet seen in a sequenced frame.
type Client struct {
	docID string
	id    crdt.ClientID
	opts  Options
	life  context.Context
	stop  context.CancelFunc

	mu        sync.Mutex
	changed   chan struct{}
	state     state
	err       error
	gen       uint64
	link      *link
	urlIdx    int
	replica   *crdt.Replica
	seq       uint64
	lastJoin  JoinKind
	unacked   []crdt.Op
	sentTimes map[crdt.ID]time.Time
	received  map[crdt.ID]time.Time
	latencies []time.Duration
	errs      []*collabv1.ErrorMessage

	name          string
	presence      map[crdt.ClientID]PresenceView
	departed      map[crdt.ClientID]departure
	joinAwareness *collabv1.AwarenessSync
}

// Cursor is a selection from Anchor to Head, both CRDT IDs. An ID this
// client does not hold yet stays as given and resolves when the character
// arrives. Set is false when the user has not placed a cursor.
type Cursor struct {
	Anchor crdt.ID
	Head   crdt.ID
	Set    bool
}

// PresenceView is another client's presence as of Version, the document's
// presence version when it last changed.
type PresenceView struct {
	Cursor  Cursor
	Name    string
	Version uint64
}

// departure is the tombstone of a client that left: presence older than
// version is stale.
type departure struct {
	version uint64
	reason  collabv1.PresenceLeft_Reason
}

// Dial connects to baseURL (ws:// or wss://) without auto-reconnect and
// waits for the initial DocumentSync before returning.
func Dial(ctx context.Context, baseURL, docID string, client crdt.ClientID) (*Client, error) {
	return DialWith(ctx, docID, client, Options{URLs: []string{baseURL}})
}

// DialWith tries each URL in order until one hub sends its DocumentSync.
func DialWith(ctx context.Context, docID string, client crdt.ClientID, opts Options) (*Client, error) {
	if len(opts.URLs) == 0 {
		return nil, errors.New("collabclient: no hub URLs")
	}
	life, stop := context.WithCancel(context.Background())
	c := &Client{
		docID:     docID,
		id:        client,
		opts:      opts,
		life:      life,
		stop:      stop,
		changed:   make(chan struct{}),
		state:     connecting,
		sentTimes: make(map[crdt.ID]time.Time),
		received:  make(map[crdt.ID]time.Time),
		presence:  make(map[crdt.ClientID]PresenceView),
		departed:  make(map[crdt.ClientID]departure),
	}
	var err error
	for i, u := range opts.URLs {
		if err = c.handshake(ctx, i, u, c.gen); err == nil || terminal(err) {
			break
		}
	}
	if err != nil {
		stop()
		return nil, err
	}
	return c, nil
}

// Edit runs fn against the local replica and queues the ops it returns, in
// order. They are sent at once when attached and after the next join
// otherwise.
func (c *Client) Edit(fn func(r *crdt.Replica) ([]crdt.Op, error)) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.state == failed {
		return c.err
	}
	ops, err := fn(c.replica)
	if err != nil || len(ops) == 0 {
		return err
	}
	c.unacked = append(c.unacked, ops...)
	c.transmit(ops)
	return nil
}

func (c *Client) InsertAt(index int, v rune) error {
	return c.Edit(func(r *crdt.Replica) ([]crdt.Op, error) {
		op, err := r.InsertAt(index, v)
		return []crdt.Op{op}, err
	})
}

func (c *Client) DeleteAt(index int) error {
	return c.Edit(func(r *crdt.Replica) ([]crdt.Op, error) {
		op, err := r.DeleteAt(index)
		return []crdt.Op{op}, err
	})
}

// MoveCursor sends a CursorMove anchored at the given CRDT IDs. It fails
// when the client is not attached.
func (c *Client) MoveCursor(anchor, head crdt.ID) error {
	frame := mustMarshal(&collabv1.ClientMessage{
		Payload: &collabv1.ClientMessage_CursorMove{CursorMove: &collabv1.CursorMove{
			Anchor: &crdtv1.OpID{Client: uint64(anchor.Client), Clock: anchor.Clock},
			Head:   &crdtv1.OpID{Client: uint64(head.Client), Clock: head.Clock},
		}},
	})
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.link == nil || !c.link.send(frame) {
		return errOffline
	}
	return nil
}

// SetName sets the display name others see. It is sent now if attached and
// again after every reconnect.
func (c *Client) SetName(name string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.name = name
	if c.link != nil {
		c.link.send(c.nameFrame())
	}
}

func (c *Client) nameFrame() []byte {
	return mustMarshal(&collabv1.ClientMessage{
		Payload: &collabv1.ClientMessage_AwarenessUpdate{AwarenessUpdate: &collabv1.AwarenessUpdate{DisplayName: c.name}},
	})
}

// Presence returns every other client this client knows to be present.
func (c *Client) Presence() map[crdt.ClientID]PresenceView {
	c.mu.Lock()
	defer c.mu.Unlock()
	return maps.Clone(c.presence)
}

// WaitPresence blocks until cond holds for the presence view, the client is
// offline with no reconnect pending, the client failed, or ctx ends. cond
// must not keep the map.
func (c *Client) WaitPresence(ctx context.Context, cond func(map[crdt.ClientID]PresenceView) bool) error {
	for {
		c.mu.Lock()
		ok, st, err, changed := cond(c.presence), c.state, c.err, c.changed
		c.mu.Unlock()
		if ok {
			return nil
		}
		if st == offline || st == failed {
			return err
		}
		select {
		case <-changed:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// LeftReason reports why client was last seen leaving, if it has left and
// not come back.
func (c *Client) LeftReason(client crdt.ClientID) (collabv1.PresenceLeft_Reason, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	d, ok := c.departed[client]
	return d.reason, ok
}

// JoinAwareness returns the first AwarenessSync of the current connection,
// or nil until it arrives.
func (c *Client) JoinAwareness() *collabv1.AwarenessSync {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.joinAwareness
}

// known is the newest version this client holds for another client, as an
// entry or a tombstone.
func (c *Client) known(client crdt.ClientID) uint64 {
	return max(c.presence[client].Version, c.departed[client].version)
}

func (c *Client) applyPresence(p *collabv1.PresenceBroadcast) {
	client := crdt.ClientID(p.GetClient())
	if client == c.id || p.GetVersion() <= c.known(client) {
		return
	}
	if left := p.GetLeft(); left != nil {
		delete(c.presence, client)
		c.departed[client] = departure{version: p.GetVersion(), reason: left.GetReason()}
		return
	}
	delete(c.departed, client)
	c.presence[client] = PresenceView{Cursor: cursorFrom(p.GetCursor()), Name: p.GetDisplayName(), Version: p.GetVersion()}
}

// applyAwareness takes every state in the sync that is newer than what this
// client holds, and drops every entry the sync omits that is not newer than
// the sync itself.
func (c *Client) applyAwareness(s *collabv1.AwarenessSync) {
	if c.joinAwareness == nil {
		c.joinAwareness = s
	}
	listed := make(map[crdt.ClientID]bool, len(s.GetStates()))
	for _, st := range s.GetStates() {
		client := crdt.ClientID(st.GetClient())
		if client == c.id {
			continue
		}
		listed[client] = true
		if st.GetVersion() <= c.known(client) {
			continue
		}
		delete(c.departed, client)
		c.presence[client] = PresenceView{Cursor: cursorFrom(st.GetCursor()), Name: st.GetDisplayName(), Version: st.GetVersion()}
	}
	for client, v := range c.presence {
		if !listed[client] && v.Version <= s.GetVersion() {
			delete(c.presence, client)
			c.departed[client] = departure{version: s.GetVersion()}
		}
	}
}

func cursorFrom(m *collabv1.CursorMove) Cursor {
	if m == nil {
		return Cursor{}
	}
	return Cursor{
		Anchor: crdt.ID{Client: crdt.ClientID(m.GetAnchor().GetClient()), Clock: m.GetAnchor().GetClock()},
		Head:   crdt.ID{Client: crdt.ClientID(m.GetHead().GetClient()), Clock: m.GetHead().GetClock()},
		Set:    true,
	}
}

// transmit sends ops on the current link in batches the hub accepts. Ops
// that cannot be sent stay unacked for the next join. A resent op keeps the
// time it was first sent.
func (c *Client) transmit(ops []crdt.Op) {
	if c.link == nil {
		return
	}
	now := time.Now()
	for batch := range slices.Chunk(ops, maxOpsPerBatch) {
		pb := &collabv1.OperationBatch{Operations: make([]*crdtv1.Operation, len(batch))}
		for i, op := range batch {
			pb.Operations[i] = crdt.OpToProto(op)
			if _, ok := c.sentTimes[op.OpID()]; !ok {
				c.sentTimes[op.OpID()] = now
			}
		}
		if !c.link.send(mustMarshal(&collabv1.ClientMessage{
			Payload: &collabv1.ClientMessage_OperationBatch{OperationBatch: pb},
		})) {
			return
		}
	}
}

func (l *link) send(frame []byte) bool {
	select {
	case l.out <- frame:
		return true
	case <-l.ctx.Done():
		return false
	}
}

// Read runs fn with the replica locked.
func (c *Client) Read(fn func(r *crdt.Replica)) {
	c.mu.Lock()
	defer c.mu.Unlock()
	fn(c.replica)
}

func (c *Client) Text() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.replica.Text()
}

// Seq returns the highest seq of the hub's op stream this client has applied.
func (c *Client) Seq() uint64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.seq
}

func (c *Client) Snapshot() *crdtv1.Snapshot {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.replica.Snapshot()
}

// Unacked returns how many of this client's ops it has not yet seen sequenced.
func (c *Client) Unacked() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.unacked)
}

// LastJoin reports how the hub answered the most recent join.
func (c *Client) LastJoin() JoinKind {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.lastJoin
}

// Connected reports whether the client is attached to a hub.
func (c *Client) Connected() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.state == attached
}

// Latencies returns the time from first sending each op to seeing it
// sequenced.
func (c *Client) Latencies() []time.Duration {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]time.Duration(nil), c.latencies...)
}

func (c *Client) SendTimes() map[crdt.ID]time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	times := make(map[crdt.ID]time.Time, len(c.sentTimes))
	for id, at := range c.sentTimes {
		times[id] = at
	}
	return times
}

func (c *Client) ReceiveTimes() map[crdt.ID]time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	times := make(map[crdt.ID]time.Time, len(c.received))
	for id, at := range c.received {
		times[id] = at
	}
	return times
}

func (c *Client) Errors() []*collabv1.ErrorMessage {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]*collabv1.ErrorMessage(nil), c.errs...)
}

// WaitUntil blocks until cond holds for the replica, the client is offline
// with no reconnect pending, the client failed, or ctx ends.
func (c *Client) WaitUntil(ctx context.Context, cond func(r *crdt.Replica) bool) error {
	for {
		c.mu.Lock()
		ok, st, err, changed := cond(c.replica), c.state, c.err, c.changed
		c.mu.Unlock()
		if ok {
			return nil
		}
		if st == offline || st == failed {
			return err
		}
		select {
		case <-changed:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// WaitAcked blocks until every op this client made has been sequenced.
func (c *Client) WaitAcked(ctx context.Context) error {
	return c.WaitUntil(ctx, func(*crdt.Replica) bool { return len(c.unacked) == 0 })
}

// Disconnect drops the socket. The client stays usable offline and does not
// reconnect until Reconnect.
func (c *Client) Disconnect() {
	c.mu.Lock()
	if c.state == failed {
		c.mu.Unlock()
		return
	}
	l := c.drop()
	c.state, c.err = offline, errDisconnected
	c.notify()
	c.mu.Unlock()
	if l != nil {
		<-l.done
	}
}

// Reconnect drops any current socket and joins the hub at hubURL, resuming
// from Seq. It returns once the client is attached and has resent its
// unacked ops. On failure the client stays offline.
func (c *Client) Reconnect(ctx context.Context, hubURL string) error {
	c.mu.Lock()
	if c.state == failed {
		err := c.err
		c.mu.Unlock()
		return err
	}
	l := c.drop()
	c.state = connecting
	gen := c.gen
	c.mu.Unlock()
	if l != nil {
		<-l.done
	}
	err := c.handshake(ctx, slices.Index(c.opts.URLs, hubURL), hubURL, gen)
	if err != nil {
		c.mu.Lock()
		if c.gen == gen && c.state == connecting {
			c.state, c.err = offline, err
			c.notify()
		}
		c.mu.Unlock()
	}
	return err
}

func (c *Client) Close() error {
	c.mu.Lock()
	l := c.link
	c.link = nil
	c.gen++
	c.state, c.err = failed, errClosed
	c.notify()
	c.mu.Unlock()
	var err error
	if l != nil {
		err = l.conn.Close(websocket.StatusNormalClosure, "")
	}
	c.stop()
	if l != nil {
		l.cancel()
		<-l.done
	}
	return err
}

// drop detaches the current link and moves to a new generation, so the old
// link's goroutines stop touching the client.
func (c *Client) drop() *link {
	l := c.link
	c.link = nil
	c.gen++
	if l != nil {
		l.cancel()
		_ = l.conn.CloseNow()
	}
	return l
}

func (c *Client) notify() {
	close(c.changed)
	c.changed = make(chan struct{})
}

// handshake dials hubURL and applies the join frames until the client is
// attached. It gives up once the client has moved past generation gen.
func (c *Client) handshake(ctx context.Context, urlIdx int, hubURL string, gen uint64) error {
	c.mu.Lock()
	if c.gen != gen {
		c.mu.Unlock()
		return errSuperseded
	}
	target, err := c.joinURL(hubURL)
	c.mu.Unlock()
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(ctx, handshakeTimeout)
	defer cancel()
	conn, _, err := websocket.Dial(ctx, target, nil)
	if err != nil {
		return err
	}
	conn.SetReadLimit(readLimit)
	for {
		msg, err := readServerMessage(ctx, conn)
		c.mu.Lock()
		if err == nil && c.gen != gen {
			err = errSuperseded
		}
		var done bool
		if err == nil {
			done, err = c.joinFrame(msg)
		}
		if err != nil {
			if c.gen == gen && terminal(err) {
				c.state, c.err = failed, err
				c.notify()
			}
			c.mu.Unlock()
			_ = conn.CloseNow()
			return err
		}
		if done {
			c.attach(conn, urlIdx)
		}
		c.notify()
		c.mu.Unlock()
		if done {
			return nil
		}
	}
}

func (c *Client) joinURL(base string) (string, error) {
	u, err := url.Parse(base)
	if err != nil {
		return "", err
	}
	u = u.JoinPath("ws", c.docID)
	q := url.Values{"client": {strconv.FormatUint(uint64(c.id), 10)}}
	if c.replica != nil {
		q.Set("seq", strconv.FormatUint(c.seq, 10))
	}
	u.RawQuery = q.Encode()
	return u.String(), nil
}

// joinFrame applies one frame of a join and reports whether the client is
// now attached. MissedOps may span several frames that all carry the seq
// they end at.
func (c *Client) joinFrame(msg *collabv1.ServerMessage) (bool, error) {
	switch p := msg.GetPayload().(type) {
	case *collabv1.ServerMessage_DocumentSync:
		if err := c.rebuild(p.DocumentSync); err != nil {
			return false, err
		}
		c.lastJoin = Sync
		return true, nil
	case *collabv1.ServerMessage_MissedOps:
		if c.replica == nil {
			return false, fmt.Errorf("%w: MissedOps for a client that never joined", errBrokenStream)
		}
		if err := c.applyMissed(p.MissedOps); err != nil {
			return false, err
		}
		c.lastJoin = Missed
		return c.seq == p.MissedOps.GetSeq(), nil
	case *collabv1.ServerMessage_Error:
		c.errs = append(c.errs, p.Error)
		return false, errorFrame(p.Error)
	default:
		return false, fmt.Errorf("%w: join began with %T", errBrokenStream, p)
	}
}

// rebuild replaces the replica with the snapshot plus the unacked ops it
// does not hold, re-applied in order. The ones it holds are acked.
func (c *Client) rebuild(sync *collabv1.DocumentSync) error {
	doc, err := crdt.FromSnapshot(sync.GetSnapshot())
	if err != nil {
		return fmt.Errorf("%w: %v", errBrokenStream, err)
	}
	kept := make([]crdt.Op, 0, len(c.unacked))
	for _, op := range c.unacked {
		if doc.Contains(op.OpID()) {
			continue
		}
		if _, err := doc.Apply(op); err != nil {
			return fmt.Errorf("%w: unacked op %s: %v", errBrokenStream, op.OpID(), err)
		}
		kept = append(kept, op)
	}
	replica, err := crdt.NewReplica(c.id, doc)
	if err != nil {
		return err
	}
	c.replica, c.unacked, c.seq = replica, kept, sync.GetSeq()
	return nil
}

func (c *Client) attach(conn *websocket.Conn, urlIdx int) {
	ctx, cancel := context.WithCancel(c.life)
	l := &link{gen: c.gen, conn: conn, out: make(chan []byte, 1024), ctx: ctx, cancel: cancel, done: make(chan struct{})}
	c.link, c.state, c.err = l, attached, nil
	c.joinAwareness = nil
	if urlIdx >= 0 {
		c.urlIdx = urlIdx
	}
	go c.readLoop(l)
	go c.writeLoop(l)
	if c.name != "" {
		l.send(c.nameFrame())
	}
	c.transmit(c.unacked)
}

func (c *Client) readLoop(l *link) {
	defer close(l.done)
	for {
		msg, err := readServerMessage(l.ctx, l.conn)
		c.mu.Lock()
		if c.gen != l.gen {
			c.mu.Unlock()
			return
		}
		if err == nil {
			err = c.handle(msg)
		}
		if err != nil {
			c.lost(err)
			c.mu.Unlock()
			return
		}
		c.notify()
		c.mu.Unlock()
	}
}

// lost ends the current link after err and decides what happens next.
func (c *Client) lost(err error) {
	c.drop()
	switch {
	case terminal(err):
		c.state = failed
	case c.opts.AutoReconnect:
		c.state = connecting
		go c.reconnectLoop(c.gen, c.urlIdx)
	default:
		c.state = offline
	}
	c.err = err
	c.notify()
}

// reconnectLoop joins the next URL after from, then the one after, with
// jittered backoff, until a join succeeds or the client leaves generation gen.
func (c *Client) reconnectLoop(gen uint64, from int) {
	backoff := minBackoff
	for i := 1; ; i++ {
		select {
		case <-time.After(backoff/2 + rand.N(backoff/2)):
		case <-c.life.Done():
			return
		}
		idx := (from + i) % len(c.opts.URLs)
		err := c.handshake(c.life, idx, c.opts.URLs[idx], gen)
		if err == nil {
			return
		}
		c.mu.Lock()
		current := c.gen == gen && c.state == connecting
		if current {
			c.err = err
		}
		c.mu.Unlock()
		if !current {
			return
		}
		backoff = min(2*backoff, maxBackoff)
	}
}

func (c *Client) handle(msg *collabv1.ServerMessage) error {
	switch p := msg.GetPayload().(type) {
	case *collabv1.ServerMessage_OperationBroadcast:
		b := p.OperationBroadcast
		return c.applySequenced(b.GetFirstSeq(), b.GetOperations())
	case *collabv1.ServerMessage_MissedOps:
		return c.applyMissed(p.MissedOps)
	case *collabv1.ServerMessage_PresenceBroadcast:
		c.applyPresence(p.PresenceBroadcast)
	case *collabv1.ServerMessage_AwarenessSync:
		c.applyAwareness(p.AwarenessSync)
	case *collabv1.ServerMessage_Error:
		c.errs = append(c.errs, p.Error)
		return errorFrame(p.Error)
	}
	return nil
}

func (c *Client) applyMissed(m *collabv1.MissedOps) error {
	if err := c.applySequenced(m.GetFirstSeq(), m.GetOperations()); err != nil {
		return err
	}
	if c.seq > m.GetSeq() {
		return fmt.Errorf("%w: MissedOps reached seq %d past its seq %d", errBrokenStream, c.seq, m.GetSeq())
	}
	return nil
}

// applySequenced applies ops that must continue the stream at c.seq+1 and
// acks this client's own ops among them.
func (c *Client) applySequenced(firstSeq uint64, ops []*crdtv1.Operation) error {
	if firstSeq != c.seq+1 {
		return fmt.Errorf("%w: hub sent seq %d after seq %d", errBrokenStream, firstSeq, c.seq)
	}
	now := time.Now()
	for _, pop := range ops {
		op, err := crdt.OpFromProto(pop)
		if err == nil {
			_, err = c.replica.Apply(op)
		}
		if err != nil {
			return fmt.Errorf("%w: seq %d: %v", errBrokenStream, c.seq+1, err)
		}
		c.seq++
		if op.OpID().Client != c.id {
			c.received[op.OpID()] = now
			continue
		}
		c.ack(op.OpID(), now)
	}
	return nil
}

func (c *Client) ack(id crdt.ID, now time.Time) {
	i := slices.IndexFunc(c.unacked, func(op crdt.Op) bool { return op.OpID() == id })
	switch {
	case i < 0:
		return
	case i == 0:
		c.unacked = c.unacked[1:]
	default:
		c.unacked = slices.Delete(c.unacked, i, i+1)
	}
	if at, ok := c.sentTimes[id]; ok {
		c.latencies = append(c.latencies, now.Sub(at))
	}
}

func (c *Client) writeLoop(l *link) {
	for {
		select {
		case frame := <-l.out:
			if err := l.conn.Write(l.ctx, websocket.MessageBinary, frame); err != nil {
				_ = l.conn.CloseNow()
				return
			}
		case <-l.ctx.Done():
			return
		}
	}
}

type hubError struct {
	code    collabv1.ErrorMessage_Code
	message string
}

func (e *hubError) Error() string {
	return fmt.Sprintf("collabclient: hub sent %s: %s", e.code, e.message)
}

func terminal(err error) bool {
	if errors.Is(err, errBrokenStream) {
		return true
	}
	var e *hubError
	return errors.As(err, &e) && e.code != collabv1.ErrorMessage_CODE_UNAVAILABLE
}

func errorFrame(e *collabv1.ErrorMessage) error {
	return &hubError{code: e.GetCode(), message: e.GetMessage()}
}

func readServerMessage(ctx context.Context, conn *websocket.Conn) (*collabv1.ServerMessage, error) {
	typ, data, err := conn.Read(ctx)
	if err != nil {
		return nil, err
	}
	if typ != websocket.MessageBinary {
		return nil, errors.New("collabclient: hub sent a text frame")
	}
	msg := &collabv1.ServerMessage{}
	if err := proto.Unmarshal(data, msg); err != nil {
		return nil, err
	}
	return msg, nil
}

func mustMarshal(msg *collabv1.ClientMessage) []byte {
	b, err := proto.Marshal(msg)
	if err != nil {
		panic(err)
	}
	return b
}
