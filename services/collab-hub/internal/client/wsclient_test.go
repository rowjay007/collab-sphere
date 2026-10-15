package client

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"sync"
	"testing"

	"github.com/coder/websocket"
	"google.golang.org/protobuf/proto"

	collabv1 "github.com/rowjay007/collab-sphere/gen/collab/v1"
	crdtv1 "github.com/rowjay007/collab-sphere/gen/crdt/v1"
	"github.com/rowjay007/collab-sphere/pkg/crdt"
	"github.com/rowjay007/collab-sphere/services/collab-hub/internal/fanout"
	"github.com/rowjay007/collab-sphere/services/collab-hub/internal/hub"
)

type testClient struct {
	t           *testing.T
	conn        *websocket.Conn
	replica     *crdt.Replica
	mu          sync.Mutex
	outstanding map[crdt.ID]struct{}
	seen        map[crdt.ID]struct{}
	errs        []*collabv1.ErrorMessage
	presence    []*collabv1.PresenceBroadcast
	pongs       []uint64
	syncMsg     *collabv1.DocumentSync
	changed     chan struct{}
	readErr     error
}

func startHub(t *testing.T) (*httptest.Server, *hub.MemoryStore) {
	t.Helper()
	store := hub.NewMemoryStore()
	reg := hub.NewRegistry(store, fanout.NewMemory())
	t.Cleanup(reg.Close)
	mux := http.NewServeMux()
	mux.Handle("GET /ws/{docID}", Handler(reg))
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv, store
}

func wsURL(srv *httptest.Server, docID string, id crdt.ClientID) string {
	u, err := url.Parse(srv.URL)
	if err != nil {
		panic(err)
	}
	u.Scheme = "ws"
	u.Path = "/ws/" + docID
	u.RawQuery = url.Values{"client": {strconv.FormatUint(uint64(id), 10)}}.Encode()
	return u.String()
}

func dial(t *testing.T, srv *httptest.Server, docID string, id crdt.ClientID) *testClient {
	t.Helper()
	c, err := dialErr(t.Context(), srv, docID, id)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return c
}

func dialErr(ctx context.Context, srv *httptest.Server, docID string, id crdt.ClientID) (*testClient, error) {
	conn, _, err := websocket.Dial(ctx, wsURL(srv, docID, id), nil)
	if err != nil {
		return nil, err
	}
	conn.SetReadLimit(1 << 20)
	c := &testClient{
		conn:        conn,
		outstanding: make(map[crdt.ID]struct{}),
		seen:        make(map[crdt.ID]struct{}),
		changed:     make(chan struct{}),
	}
	first, err := c.readMessage(ctx)
	if err != nil {
		conn.CloseNow()
		return nil, err
	}
	sync := first.GetDocumentSync()
	if sync == nil {
		conn.CloseNow()
		if e := first.GetError(); e != nil {
			return nil, &joinError{err: e}
		}
		return nil, errors.New("first message was not DocumentSync")
	}
	doc, err := crdt.FromSnapshot(sync.GetSnapshot())
	if err != nil {
		conn.CloseNow()
		return nil, err
	}
	rep, err := crdt.NewReplica(id, doc)
	if err != nil {
		conn.CloseNow()
		return nil, err
	}
	c.replica = rep
	c.syncMsg = sync
	go c.readLoop()
	return c, nil
}

type joinError struct{ err *collabv1.ErrorMessage }

func (e *joinError) Error() string { return e.err.GetCode().String() + ": " + e.err.GetMessage() }

func (c *testClient) readLoop() {
	for {
		msg, err := c.readMessage(context.Background())
		c.mu.Lock()
		if err != nil {
			c.readErr = err
			c.broadcast()
			c.mu.Unlock()
			return
		}
		c.handle(msg)
		c.broadcast()
		c.mu.Unlock()
	}
}

func (c *testClient) handle(msg *collabv1.ServerMessage) {
	switch p := msg.GetPayload().(type) {
	case *collabv1.ServerMessage_OperationBroadcast:
		for _, pb := range p.OperationBroadcast.GetOperations() {
			op, err := crdt.OpFromProto(pb)
			if err != nil {
				c.readErr = err
				return
			}
			if _, err := c.replica.Apply(op); err != nil {
				c.readErr = err
				return
			}
			c.seen[op.OpID()] = struct{}{}
			delete(c.outstanding, op.OpID())
		}
	case *collabv1.ServerMessage_PresenceBroadcast:
		c.presence = append(c.presence, p.PresenceBroadcast)
	case *collabv1.ServerMessage_Error:
		c.errs = append(c.errs, p.Error)
	case *collabv1.ServerMessage_Pong:
		c.pongs = append(c.pongs, p.Pong.GetNonce())
	case *collabv1.ServerMessage_DocumentSync:
		c.syncMsg = p.DocumentSync
	}
}

func (c *testClient) broadcast() {
	close(c.changed)
	c.changed = make(chan struct{})
}

func (c *testClient) readMessage(ctx context.Context) (*collabv1.ServerMessage, error) {
	typ, data, err := c.conn.Read(ctx)
	if err != nil {
		return nil, err
	}
	if typ != websocket.MessageBinary {
		return nil, errors.New("non-binary frame")
	}
	msg := &collabv1.ServerMessage{}
	if err := proto.Unmarshal(data, msg); err != nil {
		return nil, err
	}
	return msg, nil
}

func (c *testClient) sendClient(msg *collabv1.ClientMessage) error {
	frame, err := proto.Marshal(msg)
	if err != nil {
		return err
	}
	return c.conn.Write(context.Background(), websocket.MessageBinary, frame)
}

func (c *testClient) sendOps(ops []crdt.Op) error {
	return c.sendOpsTracked(ops, true)
}

func (c *testClient) sendRawOps(ops []crdt.Op) error {
	return c.sendOpsTracked(ops, false)
}

func (c *testClient) sendOpsTracked(ops []crdt.Op, track bool) error {
	pb := make([]*crdtv1.Operation, len(ops))
	c.mu.Lock()
	for i, op := range ops {
		pb[i] = crdt.OpToProto(op)
		if track {
			c.outstanding[op.OpID()] = struct{}{}
		}
	}
	c.mu.Unlock()
	return c.sendClient(&collabv1.ClientMessage{
		Payload: &collabv1.ClientMessage_OperationBatch{OperationBatch: &collabv1.OperationBatch{Operations: pb}},
	})
}

func (c *testClient) insertAt(index int, v rune) (crdt.Insert, error) {
	c.mu.Lock()
	op, err := c.replica.InsertAt(index, v)
	c.mu.Unlock()
	if err != nil {
		return crdt.Insert{}, err
	}
	return op, c.sendOps([]crdt.Op{op})
}

func (c *testClient) deleteAt(index int) (crdt.Delete, error) {
	c.mu.Lock()
	op, err := c.replica.DeleteAt(index)
	c.mu.Unlock()
	if err != nil {
		return crdt.Delete{}, err
	}
	return op, c.sendOps([]crdt.Op{op})
}

func (c *testClient) sendCursor(anchor, head crdt.ID) error {
	return c.sendClient(&collabv1.ClientMessage{
		Payload: &collabv1.ClientMessage_CursorMove{CursorMove: &collabv1.CursorMove{
			Anchor: &crdtv1.OpID{Client: uint64(anchor.Client), Clock: anchor.Clock},
			Head:   &crdtv1.OpID{Client: uint64(head.Client), Clock: head.Clock},
		}},
	})
}

func (c *testClient) sendPing(nonce uint64) error {
	return c.sendClient(&collabv1.ClientMessage{
		Payload: &collabv1.ClientMessage_Ping{Ping: &collabv1.Ping{Nonce: nonce}},
	})
}

func (c *testClient) waitUntil(ctx context.Context, cond func(*testClient) bool) error {
	for {
		c.mu.Lock()
		ok := cond(c)
		err := c.readErr
		ch := c.changed
		c.mu.Unlock()
		if ok {
			return nil
		}
		if err != nil {
			return err
		}
		select {
		case <-ch:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (c *testClient) waitEchoed(ctx context.Context) error {
	return c.waitUntil(ctx, func(tc *testClient) bool { return len(tc.outstanding) == 0 })
}

func (c *testClient) waitHas(ctx context.Context, ids []crdt.ID) error {
	return c.waitUntil(ctx, func(tc *testClient) bool {
		for _, id := range ids {
			if _, ok := tc.seen[id]; !ok {
				if tc.replica.Has(id) && !id.IsRoot() {
					continue
				}
				if !tc.replica.Has(id) {
					return false
				}
			}
		}
		return true
	})
}

func (c *testClient) waitError(ctx context.Context, code collabv1.ErrorMessage_Code) error {
	return c.waitUntil(ctx, func(tc *testClient) bool {
		for _, e := range tc.errs {
			if e.GetCode() == code {
				return true
			}
		}
		return false
	})
}

func (c *testClient) waitPong(ctx context.Context, nonce uint64) error {
	return c.waitUntil(ctx, func(tc *testClient) bool {
		for _, n := range tc.pongs {
			if n == nonce {
				return true
			}
		}
		return false
	})
}

func (c *testClient) waitPresence(ctx context.Context, pred func(*collabv1.PresenceBroadcast) bool) error {
	return c.waitUntil(ctx, func(tc *testClient) bool {
		for _, p := range tc.presence {
			if pred(p) {
				return true
			}
		}
		return false
	})
}

func (c *testClient) text() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.replica.Text()
}

func (c *testClient) snapshot() *crdtv1.Snapshot {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.replica.Snapshot()
}

func (c *testClient) Close() error {
	err := c.conn.Close(websocket.StatusNormalClosure, "")
	return err
}
