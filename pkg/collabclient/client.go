// Package collabclient is a Go client for the collab-hub WebSocket protocol.
// It keeps a local CRDT replica, applies local edits optimistically, and
// applies every broadcast from the hub.
package collabclient

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"strconv"
	"sync"
	"time"

	"github.com/coder/websocket"
	"google.golang.org/protobuf/proto"

	collabv1 "github.com/rowjay007/collab-sphere/gen/collab/v1"
	crdtv1 "github.com/rowjay007/collab-sphere/gen/crdt/v1"
	"github.com/rowjay007/collab-sphere/pkg/crdt"
)

type Client struct {
	conn *websocket.Conn
	out  chan []byte

	mu        sync.Mutex
	changed   chan struct{}
	replica   *crdt.Replica
	sentAt    map[crdt.ID]time.Time
	sentTimes map[crdt.ID]time.Time
	received  map[crdt.ID]time.Time
	latencies []time.Duration
	errs      []*collabv1.ErrorMessage
	readErr   error

	ctx    context.Context
	cancel context.CancelFunc
	done   chan struct{}
}

// Dial connects to baseURL (ws:// or wss://) and waits for the initial
// DocumentSync before returning.
func Dial(ctx context.Context, baseURL, docID string, client crdt.ClientID) (*Client, error) {
	u, err := url.Parse(baseURL)
	if err != nil {
		return nil, err
	}
	u = u.JoinPath("ws", docID)
	u.RawQuery = url.Values{"client": {strconv.FormatUint(uint64(client), 10)}}.Encode()

	conn, _, err := websocket.Dial(ctx, u.String(), nil)
	if err != nil {
		return nil, err
	}
	conn.SetReadLimit(64 << 20)

	first, err := readServerMessage(ctx, conn)
	if err != nil {
		conn.CloseNow()
		return nil, err
	}
	sync := first.GetDocumentSync()
	if sync == nil {
		conn.CloseNow()
		if e := first.GetError(); e != nil {
			return nil, fmt.Errorf("collabclient: hub refused join: %s: %s", e.GetCode(), e.GetMessage())
		}
		return nil, errors.New("collabclient: first message was not DocumentSync")
	}
	doc, err := crdt.FromSnapshot(sync.GetSnapshot())
	if err != nil {
		conn.CloseNow()
		return nil, err
	}
	replica, err := crdt.NewReplica(client, doc)
	if err != nil {
		conn.CloseNow()
		return nil, err
	}

	runCtx, cancel := context.WithCancel(context.Background())
	c := &Client{
		conn:      conn,
		out:       make(chan []byte, 1024),
		changed:   make(chan struct{}),
		replica:   replica,
		sentAt:    make(map[crdt.ID]time.Time),
		sentTimes: make(map[crdt.ID]time.Time),
		received:  make(map[crdt.ID]time.Time),
		ctx:       runCtx,
		cancel:    cancel,
		done:      make(chan struct{}),
	}
	go c.readLoop(runCtx)
	go c.writeLoop(runCtx)
	return c, nil
}

// Edit runs fn against the local replica and sends the ops it returns, in
// order, as one OperationBatch.
func (c *Client) Edit(fn func(r *crdt.Replica) ([]crdt.Op, error)) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.readErr != nil {
		return c.readErr
	}
	ops, err := fn(c.replica)
	if err != nil || len(ops) == 0 {
		return err
	}
	batch := &collabv1.OperationBatch{Operations: make([]*crdtv1.Operation, len(ops))}
	now := time.Now()
	for i, op := range ops {
		batch.Operations[i] = crdt.OpToProto(op)
		c.sentAt[op.OpID()] = now
		c.sentTimes[op.OpID()] = now
	}
	frame, err := proto.Marshal(&collabv1.ClientMessage{
		Payload: &collabv1.ClientMessage_OperationBatch{OperationBatch: batch},
	})
	if err != nil {
		return err
	}
	return c.send(frame)
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

// MoveCursor sends a CursorMove anchored at the given CRDT IDs.
func (c *Client) MoveCursor(anchor, head crdt.ID) error {
	frame, err := proto.Marshal(&collabv1.ClientMessage{
		Payload: &collabv1.ClientMessage_CursorMove{CursorMove: &collabv1.CursorMove{
			Anchor: &crdtv1.OpID{Client: uint64(anchor.Client), Clock: anchor.Clock},
			Head:   &crdtv1.OpID{Client: uint64(head.Client), Clock: head.Clock},
		}},
	})
	if err != nil {
		return err
	}
	return c.send(frame)
}

func (c *Client) send(frame []byte) error {
	select {
	case c.out <- frame:
		return nil
	case <-c.ctx.Done():
		return errors.New("collabclient: connection closed")
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

func (c *Client) Snapshot() *crdtv1.Snapshot {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.replica.Snapshot()
}

// Latencies returns the time from sending each op to receiving the hub's
// echo of it.
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

// WaitUntil blocks until cond holds for the replica, the connection fails, or
// ctx ends.
func (c *Client) WaitUntil(ctx context.Context, cond func(r *crdt.Replica) bool) error {
	for {
		c.mu.Lock()
		ok, readErr, changed := cond(c.replica), c.readErr, c.changed
		c.mu.Unlock()
		if ok {
			return nil
		}
		if readErr != nil {
			return readErr
		}
		select {
		case <-changed:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// WaitAcked blocks until the hub has echoed every op this client sent.
func (c *Client) WaitAcked(ctx context.Context) error {
	return c.WaitUntil(ctx, func(*crdt.Replica) bool { return len(c.sentAt) == 0 })
}

func (c *Client) Close() error {
	err := c.conn.Close(websocket.StatusNormalClosure, "")
	c.cancel()
	<-c.done
	return err
}

func (c *Client) readLoop(ctx context.Context) {
	defer close(c.done)
	for {
		msg, err := readServerMessage(ctx, c.conn)
		c.mu.Lock()
		if err != nil {
			c.readErr = err
		} else {
			err = c.handle(msg)
			if err != nil {
				c.readErr = err
			}
		}
		close(c.changed)
		c.changed = make(chan struct{})
		c.mu.Unlock()
		if err != nil {
			c.cancel()
			return
		}
	}
}

func (c *Client) handle(msg *collabv1.ServerMessage) error {
	switch p := msg.GetPayload().(type) {
	case *collabv1.ServerMessage_OperationBroadcast:
		now := time.Now()
		for _, pop := range p.OperationBroadcast.GetOperations() {
			op, err := crdt.OpFromProto(pop)
			if err != nil {
				return err
			}
			if _, err := c.replica.Apply(op); err != nil {
				return err
			}
			if op.OpID().Client != c.replica.Client() {
				c.received[op.OpID()] = now
			}
			if at, ok := c.sentAt[op.OpID()]; ok {
				c.latencies = append(c.latencies, now.Sub(at))
				delete(c.sentAt, op.OpID())
			}
		}
	case *collabv1.ServerMessage_Error:
		c.errs = append(c.errs, p.Error)
	}
	return nil
}

func (c *Client) writeLoop(ctx context.Context) {
	for {
		select {
		case frame := <-c.out:
			if err := c.conn.Write(ctx, websocket.MessageBinary, frame); err != nil {
				c.cancel()
				return
			}
		case <-ctx.Done():
			return
		}
	}
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
