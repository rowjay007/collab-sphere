package fanout

import (
	"bytes"
	"context"
	"crypto/rand"
	"net"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"google.golang.org/protobuf/proto"

	presencev1 "github.com/rowjay007/collab-sphere/gen/presence/v1"
	"github.com/rowjay007/collab-sphere/pkg/crdt"
)

func testRedis(t *testing.T, name string) *redis.Client {
	t.Helper()
	url := os.Getenv("COLLAB_TEST_REDIS_URL")
	if url == "" {
		t.Skip("COLLAB_TEST_REDIS_URL not set")
	}
	opts, err := redis.ParseURL(url)
	if err != nil {
		t.Fatal(err)
	}
	opts.ClientName = name
	rdb := redis.NewClient(opts)
	t.Cleanup(func() { _ = rdb.Close() })
	if err := rdb.Ping(t.Context()).Err(); err != nil {
		t.Fatal(err)
	}
	return rdb
}

func runRedis(t *testing.T, rdb *redis.Client, origin string) *Redis {
	t.Helper()
	r := NewRedis(rdb, origin)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = r.Run(ctx)
	}()
	t.Cleanup(func() {
		cancel()
		<-done
	})
	return r
}

func waitReady(t *testing.T, sub Subscription) {
	t.Helper()
	select {
	case <-sub.Ready():
	case <-time.After(5 * time.Second):
		t.Fatal("subscription never confirmed")
	}
}

func opBytes(t *testing.T, ops []crdt.Op) [][]byte {
	t.Helper()
	out := make([][]byte, len(ops))
	for i, op := range ops {
		b, err := proto.Marshal(crdt.OpToProto(op))
		if err != nil {
			t.Fatal(err)
		}
		out[i] = b
	}
	return out
}

func TestRedisDeliversToOtherInstances(t *testing.T) {
	doc := "fanout-" + rand.Text()
	a := runRedis(t, testRedis(t, "a-"+doc), "a")
	rdbB := testRedis(t, "b-"+doc)
	b := runRedis(t, rdbB, "b")
	atA, atB := newRecorder(), newRecorder()
	subA, subB := a.Join(doc, atA), b.Join(doc, atB)
	waitReady(t, subA)
	waitReady(t, subB)

	sent := Batch{FirstSeq: 7, Ops: []crdt.Op{
		crdt.Insert{ID: crdt.ID{Client: 3, Clock: 9}, Parent: crdt.Root, Value: 'é'},
		crdt.Delete{ID: crdt.ID{Client: 3, Clock: 10}, Target: crdt.ID{Client: 3, Clock: 9}},
	}}
	a.Publish(doc, sent)
	got := atB.nextBatch(t)
	if got.FirstSeq != sent.FirstSeq {
		t.Fatalf("first seq %d, want %d", got.FirstSeq, sent.FirstSeq)
	}
	want, have := opBytes(t, sent.Ops), opBytes(t, got.Ops)
	if len(have) != len(want) {
		t.Fatalf("%d ops, want %d", len(have), len(want))
	}
	for i := range want {
		if !bytes.Equal(have[i], want[i]) {
			t.Fatalf("op %d bytes differ", i)
		}
	}
	atA.expectNoBatch(t, 100*time.Millisecond)

	ev := &presencev1.Event{DocumentId: doc, Client: 4, Version: 2}
	payload, err := proto.Marshal(ev)
	if err != nil {
		t.Fatal(err)
	}
	if err := rdbB.Publish(t.Context(), PresenceChannel(doc), payload).Err(); err != nil {
		t.Fatal(err)
	}
	select {
	case got := <-atB.events:
		if !proto.Equal(got, ev) {
			t.Fatalf("presence event %v, want %v", got, ev)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("presence event never arrived")
	}

	subA.Close()
	subB.Close()
	deadline := time.Now().Add(5 * time.Second)
	for {
		n, err := rdbB.PubSubNumSub(t.Context(), DocChannel(doc), PresenceChannel(doc)).Result()
		if err != nil {
			t.Fatal(err)
		}
		if n[DocChannel(doc)] == 0 && n[PresenceChannel(doc)] == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("still subscribed after Close: %v", n)
		}
		time.Sleep(10 * time.Millisecond)
	}
	a.Publish(doc, batchAt(20))
	atB.expectNoBatch(t, 200*time.Millisecond)
}

// killSubscriber kills only this test's subscriber, found by name. CLIENT
// KILL TYPE pubsub would also cut subscribers of tests running in parallel.
func killSubscriber(t *testing.T, admin *redis.Client, name string) {
	t.Helper()
	list, err := admin.Do(t.Context(), "CLIENT", "LIST", "TYPE", "pubsub").Text()
	if err != nil {
		t.Fatal(err)
	}
	for line := range strings.Lines(list) {
		fields := strings.Fields(line)
		var id string
		named := false
		for _, f := range fields {
			k, v, _ := strings.Cut(f, "=")
			switch {
			case k == "id":
				id = v
			case k == "name" && v == name:
				named = true
			}
		}
		if named {
			if err := admin.Do(t.Context(), "CLIENT", "KILL", "ID", id).Err(); err != nil {
				t.Fatal(err)
			}
			return
		}
	}
	t.Fatalf("no pubsub client named %s", name)
}

// stallProxy forwards TCP to addr. freeze makes every open connection
// swallow traffic without closing, as a dead network path does; connections
// opened afterwards forward normally.
type stallProxy struct {
	ln     net.Listener
	mu     sync.Mutex
	frozen []*atomic.Bool
}

func newStallProxy(t *testing.T, addr string) *stallProxy {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	p := &stallProxy{ln: ln}
	t.Cleanup(func() { _ = ln.Close() })
	go func() {
		for {
			client, err := ln.Accept()
			if err != nil {
				return
			}
			server, err := net.Dial("tcp", addr)
			if err != nil {
				_ = client.Close()
				continue
			}
			frozen := &atomic.Bool{}
			p.mu.Lock()
			p.frozen = append(p.frozen, frozen)
			p.mu.Unlock()
			go pipe(client, server, frozen)
			go pipe(server, client, frozen)
		}
	}()
	return p
}

func pipe(from, to net.Conn, frozen *atomic.Bool) {
	defer from.Close()
	defer to.Close()
	buf := make([]byte, 32<<10)
	for {
		n, err := from.Read(buf)
		if err != nil {
			return
		}
		if frozen.Load() {
			continue
		}
		if _, err := to.Write(buf[:n]); err != nil {
			return
		}
	}
}

func (p *stallProxy) freeze() {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, f := range p.frozen {
		f.Store(true)
	}
}

func TestRedisReconnectsWhenPongIsMissing(t *testing.T) {
	doc := "fanout-" + rand.Text()
	direct := testRedis(t, "a-"+doc)
	a := runRedis(t, direct, "a")
	proxy := newStallProxy(t, direct.Options().Addr)
	opts := *direct.Options()
	opts.Addr = proxy.ln.Addr().String()
	opts.ClientName = "b-" + doc
	viaProxy := redis.NewClient(&opts)
	t.Cleanup(func() { _ = viaProxy.Close() })

	b := NewRedis(viaProxy, "b")
	b.pingEvery, b.pongWait = 50*time.Millisecond, 200*time.Millisecond
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = b.Run(ctx)
	}()
	t.Cleanup(func() {
		cancel()
		<-done
	})
	atB := newRecorder()
	waitReady(t, b.Join(doc, atB))
	time.Sleep(3 * b.pingEvery)
	select {
	case <-atB.resyncs:
		t.Fatal("resync while the connection was healthy")
	default:
	}

	proxy.freeze()
	select {
	case <-atB.resyncs:
	case <-time.After(5 * time.Second):
		t.Fatal("no resync after the subscriber connection stalled")
	}
	a.Publish(doc, batchAt(1))
	if got := atB.nextBatch(t).FirstSeq; got != 1 {
		t.Fatalf("delivered seq %d, want 1", got)
	}
}

func TestRedisResyncsAfterResubscribe(t *testing.T) {
	doc := "fanout-" + rand.Text()
	admin := testRedis(t, "admin-"+doc)
	a := runRedis(t, testRedis(t, "a-"+doc), "a")
	b := runRedis(t, testRedis(t, "b-"+doc), "b")
	atB := newRecorder()
	waitReady(t, b.Join(doc, atB))

	killSubscriber(t, admin, "b-"+doc)
	select {
	case <-atB.resyncs:
	case <-time.After(5 * time.Second):
		t.Fatal("no resync after the subscriber connection was killed")
	}
	a.Publish(doc, batchAt(1))
	if got := atB.nextBatch(t).FirstSeq; got != 1 {
		t.Fatalf("delivered seq %d, want 1", got)
	}
}
