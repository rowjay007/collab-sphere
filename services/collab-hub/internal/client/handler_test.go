package client

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	"math/rand/v2"

	"github.com/coder/websocket"
	"google.golang.org/protobuf/proto"

	collabv1 "github.com/rowjay007/collab-sphere/gen/collab/v1"
	crdtv1 "github.com/rowjay007/collab-sphere/gen/crdt/v1"
	"github.com/rowjay007/collab-sphere/pkg/crdt"
	"github.com/rowjay007/collab-sphere/services/collab-hub/internal/hub"
)

func TestDroppedPeerClosesWebSocket(t *testing.T) {
	peer := hub.NewPeer(1)
	accepted := make(chan struct{})
	startWrite := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		defer conn.CloseNow()
		close(accepted)
		<-startWrite
		go writePump(r.Context(), conn, peer)
		_, _, _ = conn.Read(r.Context())
	}))
	t.Cleanup(srv.Close)

	u, err := url.Parse(srv.URL)
	if err != nil {
		t.Fatal(err)
	}
	u.Scheme = "ws"
	conn, _, err := websocket.Dial(t.Context(), u.String(), nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.CloseNow() })
	<-accepted

	frame := make([]byte, 1024)
	for peer.TrySend(frame) {
	}
	peer.Close()
	close(startWrite)

	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	for {
		_, _, err = conn.Read(ctx)
		if err == nil {
			continue
		}
		if websocket.CloseStatus(err) != websocket.StatusPolicyViolation {
			t.Fatalf("close status = %v, error = %v", websocket.CloseStatus(err), err)
		}
		break
	}
}

func TestConvergence(t *testing.T) {
	srv, store := startHub(t)
	a := dial(t, srv, "doc", 1)
	b := dial(t, srv, "doc", 2)

	seed := uint64(time.Now().UnixNano())
	var idsA, idsB []crdt.ID
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		var err error
		idsA, err = randomEdits(a, seed, 200)
		if err != nil {
			t.Errorf("client 1 edits: %v", err)
		}
	}()
	go func() {
		defer wg.Done()
		var err error
		idsB, err = randomEdits(b, seed^0x9e3779b97f4a7c15, 200)
		if err != nil {
			t.Errorf("client 2 edits: %v", err)
		}
	}()
	wg.Wait()

	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	if err := a.waitEchoed(ctx); err != nil {
		t.Fatalf("client 1 wait echo (seed %d): %v", seed, err)
	}
	if err := b.waitEchoed(ctx); err != nil {
		t.Fatalf("client 2 wait echo (seed %d): %v", seed, err)
	}
	all := append(append([]crdt.ID{}, idsA...), idsB...)
	if err := a.waitHas(ctx, all); err != nil {
		t.Fatalf("client 1 missing peer ops (seed %d): %v", seed, err)
	}
	if err := b.waitHas(ctx, all); err != nil {
		t.Fatalf("client 2 missing peer ops (seed %d): %v", seed, err)
	}

	server := store.Doc(canonicalTestDocumentID("doc"))
	if server == nil {
		t.Fatal("store has no document")
	}
	if a.text() != b.text() || a.text() != server.Text() {
		t.Fatalf("texts diverged (seed %d): a=%q b=%q server=%q", seed, a.text(), b.text(), server.Text())
	}
	if !proto.Equal(a.snapshot(), b.snapshot()) || !proto.Equal(a.snapshot(), server.Snapshot()) {
		t.Fatalf("snapshots diverged (seed %d)", seed)
	}
}

func randomEdits(c *testClient, seed uint64, n int) ([]crdt.ID, error) {
	rng := rand.New(rand.NewPCG(seed, seed^1))
	ids := make([]crdt.ID, 0, n)
	for i := 0; i < n; i++ {
		var op crdt.Op
		var err error
		c.mu.Lock()
		if length := c.replica.Len(); length == 0 || rng.IntN(3) != 0 {
			idx := 0
			if length > 0 {
				idx = rng.IntN(length + 1)
			}
			op, err = c.replica.InsertAt(idx, rune('a'+rng.IntN(26)))
		} else {
			op, err = c.replica.DeleteAt(rng.IntN(length))
		}
		c.mu.Unlock()
		if err != nil {
			return ids, err
		}
		ids = append(ids, op.OpID())
		if err := c.sendOps([]crdt.Op{op}); err != nil {
			return ids, err
		}
	}
	return ids, nil
}

func TestLateJoinerSync(t *testing.T) {
	srv, _ := startHub(t)
	a := dial(t, srv, "doc", 1)
	if _, err := a.insertAt(0, 'H'); err != nil {
		t.Fatal(err)
	}
	if _, err := a.insertAt(1, 'i'); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	if err := a.waitEchoed(ctx); err != nil {
		t.Fatal(err)
	}

	b := dial(t, srv, "doc", 2)
	if b.syncMsg == nil || b.syncMsg.GetDocumentId() != canonicalTestDocumentID("doc") {
		t.Fatalf("late joiner missing DocumentSync: %#v", b.syncMsg)
	}
	if b.text() != a.text() {
		t.Fatalf("late joiner text %q want %q", b.text(), a.text())
	}
	if !proto.Equal(b.syncMsg.GetSnapshot(), a.snapshot()) {
		t.Fatal("late joiner snapshot does not match existing state")
	}
}

func TestProtocolErrors(t *testing.T) {
	srv, _ := startHub(t)
	seed := dial(t, srv, "doc", 2)
	if _, err := seed.insertAt(0, 's'); err != nil {
		t.Fatal(err)
	}
	seedCtx, seedCancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer seedCancel()
	if err := seed.waitEchoed(seedCtx); err != nil {
		t.Fatal(err)
	}
	if err := seed.Close(); err != nil {
		t.Fatal(err)
	}

	a := dial(t, srv, "doc", 1)
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()

	if err := a.sendRawOps([]crdt.Op{crdt.Insert{ID: crdt.ID{Client: 99, Clock: 1}, Parent: crdt.Root, Value: 'x'}}); err != nil {
		t.Fatal(err)
	}
	if err := a.waitError(ctx, collabv1.ErrorMessage_CODE_CLIENT_MISMATCH); err != nil {
		t.Fatalf("CLIENT_MISMATCH: %v", err)
	}
	if err := a.waitUntil(ctx, func(tc *testClient) bool { return tc.readErr != nil }); err != nil {
		t.Fatalf("CLIENT_MISMATCH close: %v", err)
	}

	a = dial(t, srv, "doc", 1)
	if err := a.sendRawOps([]crdt.Op{crdt.Insert{ID: crdt.ID{Client: 1, Clock: 2}, Parent: crdt.ID{Client: 99, Clock: 1}, Value: 'x'}}); err != nil {
		t.Fatal(err)
	}
	if err := a.waitError(ctx, collabv1.ErrorMessage_CODE_MISSING_DEPENDENCY); err != nil {
		t.Fatalf("MISSING_DEPENDENCY: %v", err)
	}
	if err := a.waitUntil(ctx, func(tc *testClient) bool { return tc.readErr != nil }); err != nil {
		t.Fatalf("MISSING_DEPENDENCY close: %v", err)
	}

	a = dial(t, srv, "doc", 1)
	inserted, err := a.insertAt(0, 'a')
	if err != nil {
		t.Fatal(err)
	}
	if err := a.waitEchoed(ctx); err != nil {
		t.Fatal(err)
	}
	if err := a.sendRawOps([]crdt.Op{crdt.Insert{ID: inserted.ID, Parent: inserted.Parent, Value: 'z'}}); err != nil {
		t.Fatal(err)
	}
	if err := a.waitError(ctx, collabv1.ErrorMessage_CODE_INVALID_OPERATION); err != nil {
		t.Fatalf("INVALID_OPERATION conflict: %v", err)
	}
	if err := a.waitUntil(ctx, func(tc *testClient) bool { return tc.readErr != nil }); err != nil {
		t.Fatalf("INVALID_OPERATION close: %v", err)
	}

	a = dial(t, srv, "doc", 1)
	if err := a.sendCursor(crdt.ID{Client: 99, Clock: 1}, crdt.ID{Client: 99, Clock: 1}); err != nil {
		t.Fatal(err)
	}
	if err := a.sendPing(41); err != nil {
		t.Fatal(err)
	}
	if err := a.waitPong(ctx, 41); err != nil {
		t.Fatalf("invalid cursor disconnected peer: %v", err)
	}

	superseded := a
	a = dial(t, srv, "doc", 1)
	var closeErr error
	if err := superseded.waitUntil(ctx, func(tc *testClient) bool { closeErr = tc.readErr; return closeErr != nil }); err != nil {
		t.Fatalf("superseded connection stayed open: %v", err)
	}
	if websocket.CloseStatus(closeErr) != websocket.StatusPolicyViolation {
		t.Fatalf("superseded connection: %v", closeErr)
	}

	if err := a.conn.Write(ctx, websocket.MessageBinary, []byte("not-a-protobuf")); err != nil {
		t.Fatal(err)
	}
	if err := a.waitError(ctx, collabv1.ErrorMessage_CODE_MALFORMED_MESSAGE); err != nil {
		t.Fatalf("MALFORMED_MESSAGE: %v", err)
	}

	b := dial(t, srv, "doc", 2)
	if err := b.sendPing(42); err != nil {
		t.Fatal(err)
	}
	if err := b.waitPong(ctx, 42); err != nil {
		t.Fatalf("pong: %v", err)
	}

	oversized := make([]*crdtv1.Operation, maxOpsPerBatch+1)
	for i := range oversized {
		oversized[i] = crdt.OpToProto(crdt.Insert{
			ID:     crdt.ID{Client: 2, Clock: uint64(i + 1)},
			Parent: crdt.Root,
			Value:  'x',
		})
	}
	if err := b.sendClient(&collabv1.ClientMessage{
		Payload: &collabv1.ClientMessage_OperationBatch{OperationBatch: &collabv1.OperationBatch{Operations: oversized}},
	}); err != nil {
		t.Fatal(err)
	}
	if err := b.waitError(ctx, collabv1.ErrorMessage_CODE_MALFORMED_MESSAGE); err != nil {
		t.Fatalf("batch limit: %v", err)
	}
}

func TestPresence(t *testing.T) {
	srv, _ := startHub(t)
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	a := dial(t, srv, "doc", 1)
	if sync, err := a.firstAwareness(ctx); err != nil || len(sync.GetStates()) != 0 {
		t.Fatalf("first joiner's AwarenessSync %v, %v", sync, err)
	}
	if err := a.sendName("ann"); err != nil {
		t.Fatal(err)
	}
	b := dial(t, srv, "doc", 2)
	sync, err := b.firstAwareness(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(sync.GetStates()) != 1 || sync.GetStates()[0].GetClient() != 1 || sync.GetStates()[0].GetCursor() != nil {
		t.Fatalf("joiner's AwarenessSync %v, want client 1 with no cursor", sync)
	}
	joinVersion := sync.GetVersion()

	if err := a.sendCursor(crdt.Root, crdt.Root); err != nil {
		t.Fatal(err)
	}
	var cursorVersion uint64
	if err := b.waitPresence(ctx, func(p *collabv1.PresenceBroadcast) bool {
		if p.GetClient() == 1 && p.GetCursor() != nil && p.GetDisplayName() == "ann" {
			cursorVersion = p.GetVersion()
			return true
		}
		return false
	}); err != nil {
		t.Fatalf("cursor relay: %v", err)
	}
	if cursorVersion <= joinVersion {
		t.Fatalf("cursor version %d not above the sync's %d", cursorVersion, joinVersion)
	}

	_ = a.Close()
	if err := b.waitPresence(ctx, func(p *collabv1.PresenceBroadcast) bool {
		return p.GetClient() == 1 && p.GetLeft().GetReason() == collabv1.PresenceLeft_REASON_LEAVE && p.GetVersion() > cursorVersion
	}); err != nil {
		t.Fatalf("left broadcast: %v", err)
	}

	c := dial(t, srv, "doc", 3)
	if err := b.sendCursor(crdt.Root, crdt.Root); err != nil {
		t.Fatal(err)
	}
	if err := c.waitPresence(ctx, func(p *collabv1.PresenceBroadcast) bool { return p.GetClient() == 2 && p.GetCursor() != nil }); err != nil {
		t.Fatalf("cursor relay to a later joiner: %v", err)
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	for _, p := range b.presence {
		if p.GetClient() == 2 {
			t.Fatalf("client 2 was sent its own presence: %v", p)
		}
	}
}

func TestPresenceJoinerSeesCursorAndName(t *testing.T) {
	srv, _ := startHub(t)
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	b := dial(t, srv, "doc", 2)
	if err := b.sendName("bea"); err != nil {
		t.Fatal(err)
	}
	if err := b.sendCursor(crdt.Root, crdt.Root); err != nil {
		t.Fatal(err)
	}
	if err := b.sendPing(1); err != nil {
		t.Fatal(err)
	}
	if err := b.waitPong(ctx, 1); err != nil {
		t.Fatal(err)
	}
	late := dial(t, srv, "doc", 3)
	sync, err := late.firstAwareness(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(sync.GetStates()) != 1 {
		t.Fatalf("joiner's AwarenessSync %v", sync)
	}
	st := sync.GetStates()[0]
	if st.GetClient() != 2 || st.GetDisplayName() != "bea" || st.GetCursor() == nil || st.GetVersion() == 0 {
		t.Fatalf("joiner's AwarenessSync state %v, want client 2 named bea with a cursor", st)
	}

	if err := late.sendName(strings.Repeat("x", maxNameBytes+1)); err != nil {
		t.Fatal(err)
	}
	if err := late.waitError(ctx, collabv1.ErrorMessage_CODE_MALFORMED_MESSAGE); err != nil {
		t.Fatalf("oversized name: %v", err)
	}
}

func TestDocumentIDCanonicalizationSharesHubOpsAndPresence(t *testing.T) {
	srv, store := startHub(t)
	lower := "123e4567-e89b-12d3-a456-426614174000"
	upper := strings.ToUpper(lower)
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()

	a := dial(t, srv, upper, 1)
	b := dial(t, srv, lower, 2)
	op, err := a.insertAt(0, 'x')
	if err != nil {
		t.Fatal(err)
	}
	if err := b.waitHas(ctx, []crdt.ID{op.ID}); err != nil {
		t.Fatalf("lowercase client did not receive uppercase client's op: %v", err)
	}
	if err := a.sendName("ann"); err != nil {
		t.Fatal(err)
	}
	if err := a.sendCursor(crdt.Root, crdt.Root); err != nil {
		t.Fatal(err)
	}
	if err := b.waitPresence(ctx, func(p *collabv1.PresenceBroadcast) bool {
		return p.GetClient() == 1 && p.GetDisplayName() == "ann" && p.GetCursor() != nil
	}); err != nil {
		t.Fatalf("lowercase client did not receive uppercase client's presence: %v", err)
	}
	if store.Doc(lower) == nil || store.Doc(upper) != nil {
		t.Fatalf("documents were not stored under the canonical id")
	}
}

func TestAppendFailStopReloads(t *testing.T) {
	srv, store := startHub(t)
	a := dial(t, srv, "doc", 1)
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	if _, err := a.insertAt(0, 'A'); err != nil {
		t.Fatal(err)
	}
	if err := a.waitEchoed(ctx); err != nil {
		t.Fatal(err)
	}
	if got := store.Doc(canonicalTestDocumentID("doc")).Text(); got != "A" {
		t.Fatalf("store text %q", got)
	}

	store.FailAppend(errors.New("disk full"))
	if _, err := a.insertAt(1, 'B'); err != nil {
		t.Fatal(err)
	}
	_ = a.waitUntil(ctx, func(tc *testClient) bool {
		if tc.readErr != nil {
			return true
		}
		for _, e := range tc.errs {
			if e.GetCode() == collabv1.ErrorMessage_CODE_UNAVAILABLE {
				return true
			}
		}
		return false
	})

	store.FailAppend(nil)
	b := dial(t, srv, "doc", 2)
	if b.text() != "A" {
		t.Fatalf("reloaded text %q want A", b.text())
	}
	if store.Loads() < 2 {
		t.Fatalf("expected a second Load after fail-stop, got %d", store.Loads())
	}
}

func TestHubLifecycle(t *testing.T) {
	srv, store := startHub(t)
	a := dial(t, srv, "life", 1)
	if a.syncMsg == nil {
		t.Fatal("missing initial sync")
	}
	if err := a.Close(); err != nil {
		t.Fatal(err)
	}

	deadline := time.Now().Add(2 * time.Second)
	var b *testClient
	var err error
	for time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
		b, err = dialErr(t.Context(), srv, "life", 2)
		if err != nil {
			continue
		}
		if store.Loads() >= 2 {
			break
		}
		_ = b.Close()
		b = nil
	}
	if b == nil {
		t.Fatalf("rejoin after stop failed: %v loads=%d", err, store.Loads())
	}
	t.Cleanup(func() { _ = b.Close() })
	if store.Loads() < 2 {
		t.Fatalf("hub did not reload after last leave: loads=%d", store.Loads())
	}

	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			c, err := dialErr(t.Context(), srv, "race", crdt.ClientID(i+1))
			if err != nil {
				t.Errorf("join %d: %v", i, err)
				return
			}
			if err := c.sendPing(uint64(i + 1)); err != nil {
				t.Errorf("ping %d: %v", i, err)
			}
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			if err := c.waitPong(ctx, uint64(i+1)); err != nil {
				t.Errorf("pong %d: %v", i, err)
			}
			_ = c.Close()
		}(i)
	}
	wg.Wait()

	late := dial(t, srv, "race", 99)
	if late.syncMsg == nil {
		t.Fatal("join after concurrent churn failed")
	}
}

func TestTextFrameRejected(t *testing.T) {
	srv, _ := startHub(t)
	c := dial(t, srv, "doc", 1)
	if err := c.conn.Write(t.Context(), websocket.MessageText, []byte("hello")); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	err := c.waitUntil(ctx, func(tc *testClient) bool { return tc.readErr != nil })
	if err != nil {
		t.Fatalf("expected close after text frame: %v", err)
	}
}

func TestInvalidClientQuery(t *testing.T) {
	srv, _ := startHub(t)
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	_, _, err := websocket.Dial(ctx, wsURL(srv, "doc", 0), nil)
	if err == nil {
		t.Fatal("expected dial to fail for client=0")
	}
}

func TestInvalidDocumentID(t *testing.T) {
	srv, _ := startHub(t)
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	_, resp, err := websocket.Dial(ctx, rawWSURL(srv, "not-a-uuid", 1), nil)
	if err == nil || resp == nil || resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("dial error %v, response %v", err, resp)
	}
}

func TestInvalidSeqQuery(t *testing.T) {
	srv, _ := startHub(t)
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	for _, seq := range []string{"", "-1", "abc"} {
		_, resp, err := websocket.Dial(ctx, wsURL(srv, "doc", 1)+"&seq="+seq, nil)
		if err == nil || resp == nil || resp.StatusCode != http.StatusBadRequest {
			t.Fatalf("seq=%q: dial error %v, response %v", seq, err, resp)
		}
	}
}
