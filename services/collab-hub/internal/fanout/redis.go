package fanout

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
	"google.golang.org/protobuf/proto"

	crdtv1 "github.com/rowjay007/collab-sphere/gen/crdt/v1"
	fanoutv1 "github.com/rowjay007/collab-sphere/gen/fanout/v1"
	presencev1 "github.com/rowjay007/collab-sphere/gen/presence/v1"
	"github.com/rowjay007/collab-sphere/pkg/crdt"
)

const (
	publishQueue     = 1024
	minBackoff       = 50 * time.Millisecond
	maxBackoff       = time.Second
	defaultPingEvery = 15 * time.Second
	defaultPongWait  = 5 * time.Second
)

// Redis is a Bus over Redis Pub/Sub. One connection subscribes to every
// joined document's doc and presence channels.
type Redis struct {
	rdb    *redis.Client
	ps     *redis.PubSub
	origin string
	pubq   chan published
	wake   chan struct{}

	pingEvery time.Duration
	pongWait  time.Duration

	mu   sync.Mutex
	docs map[string]*redisDoc
	cmds []command

	pubWarn throttle
	subWarn throttle
}

type published struct {
	docID string
	b     Batch
}

// command is a SUBSCRIBE or UNSUBSCRIBE of one document's two channels.
// Commands are sent in the order Join and Close queue them.
type command struct {
	docID     string
	subscribe bool
}

// redisDoc is one document's subscription. Ready closes once Redis confirms
// both channels; a later confirmation of the doc channel is go-redis
// resubscribing after a reconnect.
type redisDoc struct {
	sinks        map[*redisSub]struct{}
	ready        chan struct{}
	docLive      bool
	presenceLive bool
}

type redisSub struct {
	r     *Redis
	docID string
	doc   *redisDoc
	sink  Sink
	once  sync.Once
}

// NewRedis returns a bus that publishes as origin. Nothing reaches Redis
// until Run.
func NewRedis(rdb *redis.Client, origin string) *Redis {
	return &Redis{
		rdb:       rdb,
		ps:        rdb.Subscribe(context.Background()),
		origin:    origin,
		pubq:      make(chan published, publishQueue),
		wake:      make(chan struct{}, 1),
		pingEvery: defaultPingEvery,
		pongWait:  defaultPongWait,
		docs:      make(map[string]*redisDoc),
	}
}

// Run receives, subscribes and publishes until ctx ends. Redis errors are
// logged and retried; they never end Run.
func (r *Redis) Run(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	var wg sync.WaitGroup
	wg.Go(func() { r.control(ctx) })
	wg.Go(func() { r.publisher(ctx) })
	wg.Go(func() {
		<-ctx.Done()
		_ = r.ps.Close()
	})
	r.receive(ctx)
	cancel()
	wg.Wait()
	return nil
}

func (r *Redis) Join(docID string, sink Sink) Subscription {
	r.mu.Lock()
	defer r.mu.Unlock()
	d := r.docs[docID]
	if d == nil {
		d = &redisDoc{sinks: make(map[*redisSub]struct{}), ready: make(chan struct{})}
		r.docs[docID] = d
		r.enqueue(command{docID: docID, subscribe: true})
	}
	s := &redisSub{r: r, docID: docID, doc: d, sink: sink}
	d.sinks[s] = struct{}{}
	return s
}

// Publish queues b for doc:{docID} and drops it when the queue is full.
func (r *Redis) Publish(docID string, b Batch) {
	select {
	case r.pubq <- published{docID: docID, b: b}:
	default:
		r.pubWarn.log("fanout publish dropped", errors.New("publish queue full"))
	}
}

func (s *redisSub) Ready() <-chan struct{} { return s.doc.ready }

func (s *redisSub) Close() {
	s.once.Do(func() {
		r := s.r
		r.mu.Lock()
		defer r.mu.Unlock()
		delete(s.doc.sinks, s)
		if len(s.doc.sinks) == 0 && r.docs[s.docID] == s.doc {
			delete(r.docs, s.docID)
			r.enqueue(command{docID: s.docID})
		}
	})
}

func (r *Redis) enqueue(c command) {
	r.cmds = append(r.cmds, c)
	select {
	case r.wake <- struct{}{}:
	default:
	}
}

func (r *Redis) control(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-r.wake:
		}
		r.mu.Lock()
		cmds := r.cmds
		r.cmds = nil
		r.mu.Unlock()
		for _, c := range cmds {
			r.apply(ctx, c)
		}
	}
}

// apply sends one command. A SUBSCRIBE whose write fails is retried while
// the document is still joined: go-redis records the channels only after
// the write, so a reconnect inside that failed write would not restore them.
func (r *Redis) apply(ctx context.Context, c command) {
	channels := []string{DocChannel(c.docID), PresenceChannel(c.docID)}
	if !c.subscribe {
		if err := r.ps.Unsubscribe(ctx, channels...); err != nil && ctx.Err() == nil {
			r.subWarn.log("fanout unsubscribe", err)
		}
		return
	}
	for backoff := minBackoff; ; backoff = min(2*backoff, maxBackoff) {
		err := r.ps.Subscribe(ctx, channels...)
		if err == nil || ctx.Err() != nil || !r.joined(c.docID) {
			return
		}
		r.subWarn.log("fanout subscribe", err)
		if !sleep(ctx, backoff) {
			return
		}
	}
}

func (r *Redis) joined(docID string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.docs[docID] != nil
}

// receive reads the subscriber connection and checks its health: it pings
// every pingEvery, and a pong missing for pongWait means the connection is
// stalled without having failed, so it is closed and go-redis redials and
// resubscribes, which resyncs every document.
func (r *Redis) receive(ctx context.Context) {
	backoff := minBackoff
	nextPing := time.Now().Add(r.pingEvery)
	var pongDue time.Time
	for ctx.Err() == nil {
		if now := time.Now(); pongDue.IsZero() && !now.Before(nextPing) {
			if err := r.ps.Ping(ctx); err != nil && ctx.Err() == nil {
				r.subWarn.log("fanout ping", err)
			}
			pongDue, nextPing = now.Add(r.pongWait), now.Add(r.pingEvery)
		}
		msg, err := r.receiveOne(ctx, nextPing, pongDue)
		if err != nil {
			switch {
			case ctx.Err() != nil:
				return
			case pongDue.IsZero() && !time.Now().Before(nextPing):
				continue
			case !pongDue.IsZero() && !time.Now().Before(pongDue):
				r.subWarn.log("fanout pong missed, reconnecting", err)
				pongDue = time.Time{}
			default:
				r.subWarn.log("fanout receive", err)
			}
			sleep(ctx, backoff)
			backoff = min(2*backoff, maxBackoff)
			continue
		}
		backoff = minBackoff
		switch msg := msg.(type) {
		case *redis.Pong:
			pongDue = time.Time{}
		case *redis.Subscription:
			if msg.Kind == "subscribe" {
				r.confirm(msg.Channel)
			}
		case *redis.Message:
			r.dispatch(msg)
		}
	}
}

// receiveOne waits for one message. While no pong is due, the read times out
// when the next ping is due, which go-redis treats as harmless. While a pong
// is due, the read runs under a context deadline instead, and go-redis closes
// the connection when that deadline passes.
func (r *Redis) receiveOne(ctx context.Context, nextPing, pongDue time.Time) (any, error) {
	if pongDue.IsZero() {
		return r.ps.ReceiveTimeout(ctx, max(time.Until(nextPing), time.Millisecond))
	}
	ctx, cancel := context.WithDeadline(ctx, pongDue)
	defer cancel()
	return r.ps.Receive(ctx)
}

func (r *Redis) confirm(channel string) {
	docID, presence, ok := parseChannel(channel)
	if !ok {
		return
	}
	r.mu.Lock()
	d := r.docs[docID]
	if d == nil {
		r.mu.Unlock()
		return
	}
	var resync []Sink
	switch {
	case presence:
		d.presenceLive = true
	case d.docLive:
		resync = d.sinkList()
	default:
		d.docLive = true
	}
	if d.docLive && d.presenceLive {
		select {
		case <-d.ready:
		default:
			close(d.ready)
		}
	}
	r.mu.Unlock()
	for _, s := range resync {
		s.Resync()
	}
}

func (r *Redis) dispatch(msg *redis.Message) {
	docID, presence, ok := parseChannel(msg.Channel)
	if !ok {
		return
	}
	if presence {
		ev := &presencev1.Event{}
		if err := proto.Unmarshal([]byte(msg.Payload), ev); err != nil {
			r.subWarn.log("fanout presence event dropped", err)
			return
		}
		r.Loopback(docID, ev)
		return
	}
	sinks := r.sinks(docID)
	if len(sinks) == 0 {
		return
	}
	origin, b, err := decodeBatch([]byte(msg.Payload))
	if err != nil {
		r.subWarn.log("fanout batch dropped", err)
		return
	}
	if origin == r.origin {
		return
	}
	for _, s := range sinks {
		s.RemoteOps(b)
	}
}

func (r *Redis) Loopback(docID string, ev *presencev1.Event) {
	for _, s := range r.sinks(docID) {
		s.Presence(ev)
	}
}

func (r *Redis) sinks(docID string) []Sink {
	r.mu.Lock()
	defer r.mu.Unlock()
	if d := r.docs[docID]; d != nil {
		return d.sinkList()
	}
	return nil
}

func (d *redisDoc) sinkList() []Sink {
	sinks := make([]Sink, 0, len(d.sinks))
	for s := range d.sinks {
		sinks = append(sinks, s.sink)
	}
	return sinks
}

func (r *Redis) publisher(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case p := <-r.pubq:
			payload, err := encodeBatch(r.origin, p.b)
			if err == nil {
				err = r.rdb.Publish(ctx, DocChannel(p.docID), payload).Err()
			}
			if err != nil && ctx.Err() == nil {
				r.pubWarn.log("fanout publish dropped", err)
			}
		}
	}
}

func encodeBatch(origin string, b Batch) ([]byte, error) {
	ops := make([]*crdtv1.Operation, len(b.Ops))
	for i, op := range b.Ops {
		ops[i] = crdt.OpToProto(op)
	}
	return proto.Marshal(&fanoutv1.SequencedOps{Origin: origin, FirstSeq: b.FirstSeq, Operations: ops})
}

func decodeBatch(payload []byte) (string, Batch, error) {
	var msg fanoutv1.SequencedOps
	if err := proto.Unmarshal(payload, &msg); err != nil {
		return "", Batch{}, err
	}
	if msg.GetFirstSeq() == 0 || len(msg.GetOperations()) == 0 {
		return "", Batch{}, fmt.Errorf("empty batch at seq %d", msg.GetFirstSeq())
	}
	ops := make([]crdt.Op, len(msg.GetOperations()))
	for i, pb := range msg.GetOperations() {
		op, err := crdt.OpFromProto(pb)
		if err != nil {
			return "", Batch{}, fmt.Errorf("operation %d: %w", i, err)
		}
		ops[i] = op
	}
	return msg.GetOrigin(), Batch{FirstSeq: msg.GetFirstSeq(), Ops: ops}, nil
}

func parseChannel(channel string) (docID string, presence bool, ok bool) {
	if docID, ok := strings.CutPrefix(channel, DocChannel("")); ok {
		return docID, false, true
	}
	if docID, ok := strings.CutPrefix(channel, PresenceChannel("")); ok {
		return docID, true, true
	}
	return "", false, false
}

func sleep(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return true
	case <-ctx.Done():
		return false
	}
}

// throttle logs at most once per second and counts what it suppressed.
type throttle struct {
	mu         sync.Mutex
	last       time.Time
	suppressed int
}

func (t *throttle) log(msg string, err error) {
	t.mu.Lock()
	now := time.Now()
	if now.Sub(t.last) < time.Second {
		t.suppressed++
		t.mu.Unlock()
		return
	}
	suppressed := t.suppressed
	t.last, t.suppressed = now, 0
	t.mu.Unlock()
	slog.Warn(msg, "err", err, "suppressed", suppressed)
}
