package presence

import (
	"context"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/proto"

	presencev1 "github.com/rowjay007/collab-sphere/gen/presence/v1"
	"github.com/rowjay007/collab-sphere/pkg/crdt"
)

// A half-open hub A still holds client 7's session s1 and flushes a late
// cursor move after client 7 rejoined hub B with s2 and a higher epoch.
func TestStaleUpsertKeepsNewerSession(t *testing.T) {
	client, events := presenceService(t)
	ctx := t.Context()
	doc := uuid.NewString()
	a := NewWriter(client, 16*time.Millisecond, 100*time.Millisecond)
	t.Cleanup(a.Close)
	b := NewWriter(client, 16*time.Millisecond, 100*time.Millisecond)
	t.Cleanup(b.Close)
	s1 := Session{Client: 7, Session: 1001, Epoch: 1}
	s2 := Session{Client: 7, Session: 2002, Epoch: 2}

	if _, _, err := a.Join(ctx, doc, s1); err != nil {
		t.Fatal(err)
	}
	if _, _, err := b.Join(ctx, doc, s2); err != nil {
		t.Fatal(err)
	}
	seen := events(doc)
	a.Move(doc, s1, crdt.Root, crdt.Root)
	waitForgotten(t, a, doc, s1)
	b.Move(doc, s2, crdt.Root, crdt.Root)
	time.Sleep(300 * time.Millisecond)

	a.Leave(doc, s1)
	time.Sleep(300 * time.Millisecond)
	assertOnlySession(t, client, doc, s2)
	for _, ev := range seen() {
		if ev.GetLeft() != nil || ev.GetState().GetSession() == s1.Session {
			t.Fatalf("stale session published %v", ev)
		}
	}
}

func TestLateLowerEpochJoinDoesNotEvict(t *testing.T) {
	client, events := presenceService(t)
	ctx := t.Context()
	doc := uuid.NewString()
	a := NewWriter(client, 16*time.Millisecond, 100*time.Millisecond)
	t.Cleanup(a.Close)
	b := NewWriter(client, 16*time.Millisecond, 100*time.Millisecond)
	t.Cleanup(b.Close)
	s1 := Session{Client: 7, Session: 1001, Epoch: 1}
	s2 := Session{Client: 7, Session: 2002, Epoch: 2}

	if _, _, err := b.Join(ctx, doc, s2); err != nil {
		t.Fatal(err)
	}
	seen := events(doc)
	if _, _, err := a.Join(ctx, doc, s1); err != nil {
		t.Fatal(err)
	}
	waitForgotten(t, a, doc, s1)
	a.Move(doc, s1, crdt.Root, crdt.Root)
	time.Sleep(300 * time.Millisecond)
	a.Leave(doc, s1)
	time.Sleep(300 * time.Millisecond)

	assertOnlySession(t, client, doc, s2)
	if got := seen(); len(got) != 0 {
		t.Fatalf("the older connection published %v", got)
	}
}

func waitForgotten(t *testing.T, w *Writer, doc string, s Session) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		w.mu.Lock()
		e := w.live[key{doc: doc, client: s.Client}]
		gone := e != nil && e.session == s.Session && e.phase == forgotten
		w.mu.Unlock()
		if gone {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("writer never forgot the superseded session")
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func assertOnlySession(t *testing.T, client presencev1.PresenceServiceClient, doc string, s Session) {
	t.Helper()
	resp, err := client.List(t.Context(), &presencev1.ListRequest{DocumentId: doc})
	if err != nil {
		t.Fatal(err)
	}
	if st := resp.GetStates(); len(st) != 1 || st[0].GetSession() != s.Session {
		t.Fatalf("List = %v, want only session %d", st, s.Session)
	}
}

var (
	buildOnce sync.Once
	bin       string
	buildErr  error
)

// presenceService runs a presence-service binary against
// COLLAB_TEST_REDIS_URL. events subscribes to a document's presence channel
// and returns what has arrived so far.
func presenceService(t *testing.T) (presencev1.PresenceServiceClient, func(doc string) func() []*presencev1.Event) {
	t.Helper()
	redisURL := os.Getenv("COLLAB_TEST_REDIS_URL")
	if redisURL == "" {
		t.Skip("COLLAB_TEST_REDIS_URL not set")
	}
	buildOnce.Do(func() {
		dir, err := os.MkdirTemp("", "presence-service-bin")
		if err != nil {
			buildErr = err
			return
		}
		bin = filepath.Join(dir, "presence-service")
		out, err := exec.Command("go", "build", "-o", bin, "github.com/rowjay007/collab-sphere/services/presence-service/cmd/presence-service").CombinedOutput()
		if err != nil {
			buildErr = fmt.Errorf("build presence-service: %v\n%s", err, out)
		}
	})
	if buildErr != nil {
		t.Fatal(buildErr)
	}
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := l.Addr().String()
	l.Close()
	cmd := exec.Command(bin)
	cmd.Env = append(os.Environ(), "REDIS_URL="+redisURL, "GRPC_ADDR="+addr)
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
	})

	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })
	client := presencev1.NewPresenceServiceClient(conn)
	deadline := time.Now().Add(30 * time.Second)
	for {
		ctx, cancel := context.WithTimeout(t.Context(), time.Second)
		_, err := client.List(ctx, &presencev1.ListRequest{DocumentId: uuid.NewString()})
		cancel()
		if err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("presence-service never became ready: %v", err)
		}
		time.Sleep(50 * time.Millisecond)
	}

	opt, err := redis.ParseURL(redisURL)
	if err != nil {
		t.Fatal(err)
	}
	rdb := redis.NewClient(opt)
	t.Cleanup(func() { rdb.Close() })
	events := func(doc string) func() []*presencev1.Event {
		sub := rdb.Subscribe(t.Context(), "presence-events:"+doc)
		t.Cleanup(func() { sub.Close() })
		if _, err := sub.Receive(t.Context()); err != nil {
			t.Fatal(err)
		}
		var mu sync.Mutex
		var got []*presencev1.Event
		go func() {
			for msg := range sub.Channel() {
				ev := &presencev1.Event{}
				if proto.Unmarshal([]byte(msg.Payload), ev) == nil {
					mu.Lock()
					got = append(got, ev)
					mu.Unlock()
				}
			}
		}()
		return func() []*presencev1.Event {
			mu.Lock()
			defer mu.Unlock()
			return append([]*presencev1.Event(nil), got...)
		}
	}
	return client, events
}
