package client

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"

	collabv1 "github.com/rowjay007/collab-sphere/gen/collab/v1"
	"github.com/rowjay007/collab-sphere/pkg/collabclient"
	"github.com/rowjay007/collab-sphere/pkg/crdt"
	"github.com/rowjay007/collab-sphere/services/collab-hub/internal/hub"
)

func serve(t *testing.T, store hub.Store) (string, *hub.Registry) {
	t.Helper()
	reg := localRegistry(store)
	t.Cleanup(reg.Close)
	mux := http.NewServeMux()
	mux.Handle("GET /ws/{docID}", Handler(reg))
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return "ws" + strings.TrimPrefix(srv.URL, "http"), reg
}

func serveCounting(t *testing.T, store hub.Store) (string, *atomic.Int64) {
	t.Helper()
	reg := localRegistry(store)
	t.Cleanup(reg.Close)
	var attempts atomic.Int64
	handler := Handler(reg)
	mux := http.NewServeMux()
	mux.Handle("GET /ws/{docID}", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attempts.Add(1)
		handler.ServeHTTP(w, r)
	}))
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return "ws" + strings.TrimPrefix(srv.URL, "http"), &attempts
}

func dialWith(t *testing.T, ctx context.Context, id crdt.ClientID, opts collabclient.Options) *collabclient.Client {
	t.Helper()
	c, err := collabclient.DialWith(ctx, canonicalTestDocumentID("doc"), id, opts)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.Close() })
	return c
}

// edit applies one local edit and returns the op it made.
func edit(t *testing.T, c *collabclient.Client, fn func(r *crdt.Replica) (crdt.Op, error)) crdt.Op {
	t.Helper()
	var op crdt.Op
	if err := c.Edit(func(r *crdt.Replica) ([]crdt.Op, error) {
		var err error
		op, err = fn(r)
		return []crdt.Op{op}, err
	}); err != nil {
		t.Fatal(err)
	}
	return op
}

func appendAt(i int, v rune) func(r *crdt.Replica) (crdt.Op, error) {
	return func(r *crdt.Replica) (crdt.Op, error) { return r.InsertAt(min(i, r.Len()), v) }
}

// converge waits until every client is acked and holds exactly the stored log.
func converge(t *testing.T, ctx context.Context, store *hub.MemoryStore, clients ...*collabclient.Client) {
	t.Helper()
	for i, c := range clients {
		if err := c.WaitAcked(ctx); err != nil {
			t.Fatalf("client %d: wait acked: %v (unacked %d)", i, err, c.Unacked())
		}
		err := c.WaitUntil(ctx, func(r *crdt.Replica) bool {
			return proto.Equal(r.Snapshot(), store.Doc(canonicalTestDocumentID("doc")).Snapshot())
		})
		if err != nil {
			t.Fatalf("client %d: %q never matched the log %q: %v", i, c.Text(), store.Doc(canonicalTestDocumentID("doc")).Text(), err)
		}
	}
}

// loggedOnce asserts that the log holds every op in ids exactly once.
func loggedOnce(t *testing.T, store *hub.MemoryStore, ids []crdt.ID) {
	t.Helper()
	b, _, err := store.ReadOperations(context.Background(), canonicalTestDocumentID("doc"), 0, 1<<30)
	if err != nil {
		t.Fatal(err)
	}
	count := make(map[crdt.ID]int)
	for _, op := range b.Ops {
		count[op.OpID()]++
	}
	for _, id := range ids {
		if count[id] != 1 {
			t.Fatalf("log holds op %s %d times", id, count[id])
		}
	}
}

func TestClientOfflineEditsResumeWithMissedOps(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	store := hub.NewMemoryStore()
	url, _ := serve(t, store)
	x := dialWith(t, ctx, 1, collabclient.Options{URLs: []string{url}})
	y := dialWith(t, ctx, 2, collabclient.Options{URLs: []string{url}})
	ids := []crdt.ID{edit(t, x, appendAt(0, 'a')).OpID()}
	if err := x.WaitAcked(ctx); err != nil {
		t.Fatal(err)
	}

	x.Disconnect()
	if x.Connected() {
		t.Fatal("Connected() after Disconnect")
	}
	for i := range 10 {
		ids = append(ids, edit(t, x, appendAt(1+i, 'x')).OpID())
		ids = append(ids, edit(t, y, appendAt(0, 'y')).OpID())
	}
	if got := x.Unacked(); got != 10 {
		t.Fatalf("Unacked() = %d offline, want 10", got)
	}
	if err := x.Reconnect(ctx, url); err != nil {
		t.Fatal(err)
	}
	if !x.Connected() || x.LastJoin() != collabclient.Missed {
		t.Fatalf("Connected() = %v, LastJoin() = %v after Reconnect", x.Connected(), x.LastJoin())
	}
	converge(t, ctx, store, x, y)
	loggedOnce(t, store, ids)
}

func TestClientDocumentSyncReappliesUnacked(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
	defer cancel()
	store := hub.NewMemoryStore()
	urlA, _ := serve(t, store)
	urlB, _ := serve(t, store)
	x := dialWith(t, ctx, 1, collabclient.Options{URLs: []string{urlA, urlB}})
	first := edit(t, x, appendAt(0, 'a'))
	if err := x.WaitAcked(ctx); err != nil {
		t.Fatal(err)
	}

	x.Disconnect()
	offline := []crdt.Op{edit(t, x, appendAt(1, 'b')), edit(t, x, appendAt(2, 'c'))}
	offline = append(offline, edit(t, x, func(r *crdt.Replica) (crdt.Op, error) { return r.Delete(offline[0].OpID()) }))
	if _, err := store.AppendFromOtherWriter(canonicalTestDocumentID("doc"), offline[0]); err != nil {
		t.Fatal(err)
	}
	filler := make([]crdt.Op, 20_001)
	parent := crdt.Root
	for i := range filler {
		op := crdt.Insert{ID: crdt.ID{Client: 50, Clock: uint64(i + 1)}, Parent: parent, Value: 'f'}
		filler[i], parent = op, op.ID
	}
	if _, err := store.AppendFromOtherWriter(canonicalTestDocumentID("doc"), filler...); err != nil {
		t.Fatal(err)
	}

	if err := x.Reconnect(ctx, urlB); err != nil {
		t.Fatal(err)
	}
	if x.LastJoin() != collabclient.Sync {
		t.Fatalf("LastJoin() = %v, want Sync", x.LastJoin())
	}
	if got := x.Unacked(); got > 2 {
		t.Fatalf("Unacked() = %d after the snapshot held one of 3", got)
	}
	converge(t, ctx, store, x)
	ids := []crdt.ID{first.OpID()}
	for _, op := range offline {
		ids = append(ids, op.OpID())
	}
	loggedOnce(t, store, ids)
}

type gatedAppendStore struct {
	*hub.MemoryStore
	started chan struct{}
	release chan struct{}
	once    sync.Once
}

func (s *gatedAppendStore) Append(ctx context.Context, docID string, ops []crdt.Op) (hub.AppendResult, error) {
	s.once.Do(func() {
		close(s.started)
		<-s.release
	})
	return s.MemoryStore.Append(ctx, docID, ops)
}

func TestClientSentOpsInLogAreAckedByMissedOps(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	store := &gatedAppendStore{MemoryStore: hub.NewMemoryStore(), started: make(chan struct{}), release: make(chan struct{})}
	url, _ := serve(t, store)
	x := dialWith(t, ctx, 1, collabclient.Options{URLs: []string{url}})
	y := dialWith(t, ctx, 2, collabclient.Options{URLs: []string{url}})

	var sent []crdt.ID
	if err := x.Edit(func(r *crdt.Replica) ([]crdt.Op, error) {
		ops, err := r.InsertText(0, "abc")
		out := make([]crdt.Op, len(ops))
		for i, op := range ops {
			out[i] = op
			sent = append(sent, op.ID)
		}
		return out, err
	}); err != nil {
		t.Fatal(err)
	}
	<-store.started
	x.Disconnect()
	close(store.release)
	if err := y.WaitUntil(ctx, func(r *crdt.Replica) bool { return r.Len() == 3 }); err != nil {
		t.Fatal(err)
	}
	if got := x.Unacked(); got != 3 {
		t.Fatalf("Unacked() = %d before reconnect, want 3", got)
	}

	if err := x.Reconnect(ctx, url); err != nil {
		t.Fatal(err)
	}
	if x.LastJoin() != collabclient.Missed {
		t.Fatalf("LastJoin() = %v, want Missed", x.LastJoin())
	}
	if got := x.Unacked(); got != 0 {
		t.Fatalf("Unacked() = %d after MissedOps held every sent op", got)
	}
	sent = append(sent, edit(t, x, appendAt(3, 'd')).OpID())
	converge(t, ctx, store.MemoryStore, x, y)
	loggedOnce(t, store.MemoryStore, sent)
}

func TestClientAutoReconnect(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
	defer cancel()
	store := hub.NewMemoryStore()
	url, reg := serve(t, store)
	x := dialWith(t, ctx, 1, collabclient.Options{URLs: []string{url}, AutoReconnect: true})
	ids := []crdt.ID{edit(t, x, appendAt(0, 'a')).OpID()}
	if err := x.WaitAcked(ctx); err != nil {
		t.Fatal(err)
	}

	reg.Close()
	ids = append(ids, edit(t, x, appendAt(1, 'b')).OpID())
	converge(t, ctx, store, x)
	if x.LastJoin() != collabclient.Missed {
		t.Fatalf("LastJoin() = %v after the hub closed the socket, want Missed", x.LastJoin())
	}

	store.FailAppend(errors.New("disk on fire"))
	ids = append(ids, edit(t, x, appendAt(2, 'c')).OpID())
	unavailable := func() bool {
		for _, e := range x.Errors() {
			if e.GetCode() == collabv1.ErrorMessage_CODE_UNAVAILABLE {
				return true
			}
		}
		return false
	}
	for !unavailable() {
		select {
		case <-ctx.Done():
			t.Fatal("hub never reported CODE_UNAVAILABLE")
		case <-time.After(time.Millisecond):
		}
	}
	store.FailAppend(nil)
	converge(t, ctx, store, x)
	if !x.Connected() {
		t.Fatal("not connected after recovery")
	}
	loggedOnce(t, store, ids)
}

func TestClientRejectedOperationIsTerminal(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	url, attempts := serveCounting(t, hub.NewMemoryStore())
	c := dialWith(t, ctx, 1, collabclient.Options{URLs: []string{url}, AutoReconnect: true})

	missing := crdt.Insert{
		ID:     crdt.ID{Client: 1, Clock: 1},
		Parent: crdt.ID{Client: 99},
		Value:  'x',
	}
	if err := c.Edit(func(*crdt.Replica) ([]crdt.Op, error) {
		return []crdt.Op{missing}, nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := c.WaitAcked(ctx); err == nil {
		t.Fatal("WaitAcked succeeded after the hub rejected the op")
	}

	errs := c.Errors()
	if len(errs) != 1 || errs[0].GetCode() != collabv1.ErrorMessage_CODE_MISSING_DEPENDENCY {
		t.Fatalf("Errors() = %v, want one MISSING_DEPENDENCY", errs)
	}
	time.Sleep(200 * time.Millisecond)
	if got := attempts.Load(); got != 1 {
		t.Fatalf("connection attempts = %d, want 1", got)
	}
}

type archivedLoadStore struct {
	hub.Store
}

func (archivedLoadStore) Load(context.Context, string) (*crdt.Doc, uint64, error) {
	return nil, 0, hub.ErrArchived
}

func TestClientArchivedJoinIsTerminal(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	url, attempts := serveCounting(t, archivedLoadStore{Store: hub.NewMemoryStore()})

	_, err := collabclient.DialWith(ctx, canonicalTestDocumentID("doc"), 1, collabclient.Options{
		URLs:          []string{url, url},
		AutoReconnect: true,
	})
	if err == nil {
		t.Fatal("DialWith succeeded for an archived document")
	}
	if got := attempts.Load(); got != 1 {
		t.Fatalf("connection attempts = %d, want 1", got)
	}
}
