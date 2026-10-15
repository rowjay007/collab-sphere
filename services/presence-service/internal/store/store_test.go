package store_test

import (
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"

	crdtv1 "github.com/rowjay007/collab-sphere/gen/crdt/v1"
	presencev1 "github.com/rowjay007/collab-sphere/gen/presence/v1"
	"github.com/rowjay007/collab-sphere/services/presence-service/internal/broadcast"
	"github.com/rowjay007/collab-sphere/services/presence-service/internal/store"
	"github.com/rowjay007/collab-sphere/services/presence-service/internal/testredis"
)

func TestUpsertOverwritesAndPublishes(t *testing.T) {
	st, rdb := openStore(t, time.Second)
	ctx := t.Context()
	doc := uuid.NewString()
	ch := subscribe(t, rdb, doc)

	first := &presencev1.State{DocumentId: doc, Client: 7, Session: 11, DisplayName: "ann"}
	if err := st.Upsert(ctx, first); err != nil {
		t.Fatalf("upsert: %v", err)
	}
	if first.GetVersion() != 1 {
		t.Fatalf("version %d", first.GetVersion())
	}
	second := &presencev1.State{DocumentId: doc, Client: 7, Session: 99, DisplayName: "bob"}
	if err := st.Upsert(ctx, second); err != nil {
		t.Fatalf("rejoin: %v", err)
	}
	if second.GetVersion() != 2 {
		t.Fatalf("version %d", second.GetVersion())
	}

	evs := collectN(t, ch, 2, time.Second)
	if evs[0].GetVersion() != 1 || evs[0].GetState().GetSession() != 11 || evs[0].GetState().GetDisplayName() != "ann" {
		t.Fatalf("event 0 %+v", evs[0])
	}
	if evs[1].GetVersion() != 2 || evs[1].GetState().GetSession() != 99 || evs[1].GetState().GetDisplayName() != "bob" {
		t.Fatalf("event 1 %+v", evs[1])
	}
	assertNoMore(t, ch, 50*time.Millisecond)

	got, ver, err := st.List(ctx, doc)
	if err != nil {
		t.Fatal(err)
	}
	if ver != 2 || len(got) != 1 || got[0].GetSession() != 99 || got[0].GetDisplayName() != "bob" {
		t.Fatalf("list %+v ver=%d", got, ver)
	}
}

func TestTouchOutcomes(t *testing.T) {
	st, rdb := openStore(t, 300*time.Millisecond)
	ctx := t.Context()
	doc := uuid.NewString()
	ch := subscribe(t, rdb, doc)

	self := &presencev1.State{DocumentId: doc, Client: 3, Session: 1 << 60, DisplayName: "ann"}
	if err := st.Upsert(ctx, self); err != nil {
		t.Fatal(err)
	}
	collectN(t, ch, 1, time.Second)

	result, err := st.Touch(ctx, &presencev1.Touch{DocumentId: doc, Client: 3, Session: 1 << 60})
	if err != nil || result != store.TouchApplied {
		t.Fatalf("touch match result=%v err=%v", result, err)
	}
	result, err = st.Touch(ctx, &presencev1.Touch{DocumentId: doc, Client: 3, Session: 2})
	if err != nil || result != store.TouchSuperseded {
		t.Fatalf("touch wrong session result=%v err=%v", result, err)
	}
	result, err = st.Touch(ctx, &presencev1.Touch{DocumentId: doc, Client: 9, Session: 1})
	if err != nil || result != store.TouchMissing {
		t.Fatalf("touch missing key result=%v err=%v", result, err)
	}
	assertNoMore(t, ch, 50*time.Millisecond)

	time.Sleep(200 * time.Millisecond)
	result, err = st.Touch(ctx, &presencev1.Touch{DocumentId: doc, Client: 3, Session: 1 << 60})
	if err != nil || result != store.TouchApplied {
		t.Fatalf("extend: result=%v err=%v", result, err)
	}
	time.Sleep(200 * time.Millisecond)
	got, ver, err := st.List(ctx, doc)
	if err != nil {
		t.Fatal(err)
	}
	if ver != 1 || len(got) != 1 || got[0].GetSession() != 1<<60 {
		t.Fatalf("still live %+v ver=%d", got, ver)
	}
}

func TestRemoveMatchingAndStaleSession(t *testing.T) {
	st, rdb := openStore(t, time.Second)
	ctx := t.Context()
	doc := uuid.NewString()
	ch := subscribe(t, rdb, doc)

	if err := st.Upsert(ctx, &presencev1.State{DocumentId: doc, Client: 4, Session: 8, DisplayName: "ann"}); err != nil {
		t.Fatal(err)
	}
	collectN(t, ch, 1, time.Second)

	if err := st.Remove(ctx, &presencev1.Leave{DocumentId: doc, Client: 4, Session: 1}); err != nil {
		t.Fatal(err)
	}
	assertNoMore(t, ch, 50*time.Millisecond)
	got, ver, err := st.List(ctx, doc)
	if err != nil {
		t.Fatal(err)
	}
	if ver != 1 || len(got) != 1 {
		t.Fatalf("stale remove mutated %+v ver=%d", got, ver)
	}

	if err := st.Remove(ctx, &presencev1.Leave{DocumentId: doc, Client: 4, Session: 8}); err != nil {
		t.Fatal(err)
	}
	left := collectN(t, ch, 1, time.Second)[0]
	if left.GetVersion() != 2 || left.GetLeft().GetSession() != 8 || left.GetLeft().GetReason() != presencev1.Left_REASON_LEAVE {
		t.Fatalf("leave %+v", left)
	}
	got, ver, err = st.List(ctx, doc)
	if err != nil {
		t.Fatal(err)
	}
	if ver != 2 || len(got) != 0 {
		t.Fatalf("after leave %+v ver=%d", got, ver)
	}
}

func TestLargeIDsUpsertListAndRemoveExactly(t *testing.T) {
	st, rdb := openStore(t, time.Second)
	ctx := t.Context()
	doc := uuid.NewString()
	ch := subscribe(t, rdb, doc)
	client := uint64(1<<63 + 12345)
	session := uint64(1<<63 + 99)

	state := &presencev1.State{DocumentId: doc, Client: client, Session: session, DisplayName: "ann"}
	if err := st.Upsert(ctx, state); err != nil {
		t.Fatal(err)
	}
	upserted := collectN(t, ch, 1, time.Second)[0]
	if upserted.GetClient() != client || upserted.GetState().GetClient() != client || upserted.GetState().GetSession() != session {
		t.Fatalf("upsert event %+v", upserted)
	}

	got, _, err := st.List(ctx, doc)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0].GetClient() != client || got[0].GetSession() != session {
		t.Fatalf("list %+v", got)
	}

	if err := st.Remove(ctx, &presencev1.Leave{DocumentId: doc, Client: client, Session: session}); err != nil {
		t.Fatal(err)
	}
	left := collectN(t, ch, 1, time.Second)[0]
	if left.GetClient() != client || left.GetLeft().GetSession() != session {
		t.Fatalf("remove event %+v", left)
	}
}

func TestSweepExpiresOnce(t *testing.T) {
	st, rdb := openStore(t, 300*time.Millisecond)
	ctx := t.Context()
	doc := uuid.NewString()
	ch := subscribe(t, rdb, doc)

	if err := st.Upsert(ctx, &presencev1.State{DocumentId: doc, Client: 5, Session: 13, DisplayName: "ann"}); err != nil {
		t.Fatal(err)
	}
	collectN(t, ch, 1, time.Second)
	time.Sleep(400 * time.Millisecond)

	var wg sync.WaitGroup
	wg.Add(2)
	for range 2 {
		go func() {
			defer wg.Done()
			if _, err := st.Sweep(ctx, 64); err != nil {
				t.Errorf("sweep: %v", err)
			}
		}()
	}
	wg.Wait()

	exp := collectN(t, ch, 1, time.Second)[0]
	if exp.GetVersion() != 2 || exp.GetLeft().GetSession() != 13 || exp.GetLeft().GetReason() != presencev1.Left_REASON_EXPIRED {
		t.Fatalf("expired %+v", exp)
	}
	assertNoMore(t, ch, 80*time.Millisecond)

	got, ver, err := st.List(ctx, doc)
	if err != nil {
		t.Fatal(err)
	}
	if ver != 2 || len(got) != 0 {
		t.Fatalf("after sweep %+v ver=%d", got, ver)
	}
}

func TestLargeIDsSweepExactly(t *testing.T) {
	st, rdb := openStore(t, 100*time.Millisecond)
	ctx := t.Context()
	doc := uuid.NewString()
	ch := subscribe(t, rdb, doc)
	client := uint64(1<<63 + 12345)
	session := uint64(1<<63 + 99)

	if err := st.Upsert(ctx, &presencev1.State{DocumentId: doc, Client: client, Session: session}); err != nil {
		t.Fatal(err)
	}
	collectN(t, ch, 1, time.Second)
	time.Sleep(150 * time.Millisecond)
	if _, err := st.Sweep(ctx, 64); err != nil {
		t.Fatal(err)
	}
	expired := collectN(t, ch, 1, time.Second)[0]
	if expired.GetClient() != client || expired.GetLeft().GetSession() != session {
		t.Fatalf("sweep event %+v", expired)
	}
}

func TestListFiltersUnsweptExpired(t *testing.T) {
	st, _ := openStore(t, 300*time.Millisecond)
	ctx := t.Context()
	doc := uuid.NewString()
	if err := st.Upsert(ctx, &presencev1.State{DocumentId: doc, Client: 1, Session: 1, DisplayName: "ann"}); err != nil {
		t.Fatal(err)
	}
	time.Sleep(400 * time.Millisecond)
	got, ver, err := st.List(ctx, doc)
	if err != nil {
		t.Fatal(err)
	}
	if ver != 1 || len(got) != 0 {
		t.Fatalf("unswept expired %+v ver=%d", got, ver)
	}
}

func TestVersionsIncrease(t *testing.T) {
	st, _ := openStore(t, time.Second)
	ctx := t.Context()
	doc := uuid.NewString()
	a := &presencev1.State{
		DocumentId:  doc,
		Client:      1,
		Session:     1,
		DisplayName: "ann",
		HasCursor:   true,
		Anchor:      &crdtv1.OpID{Client: 1, Clock: 4},
		Head:        &crdtv1.OpID{Client: 1, Clock: 4},
	}
	if err := st.Upsert(ctx, a); err != nil {
		t.Fatal(err)
	}
	b := &presencev1.State{DocumentId: doc, Client: 2, Session: 2, DisplayName: "bob"}
	if err := st.Upsert(ctx, b); err != nil {
		t.Fatal(err)
	}
	if a.GetVersion() != 1 || b.GetVersion() != 2 {
		t.Fatalf("upsert versions %d %d", a.GetVersion(), b.GetVersion())
	}
	if err := st.Remove(ctx, &presencev1.Leave{DocumentId: doc, Client: 1, Session: 1}); err != nil {
		t.Fatal(err)
	}
	got, ver, err := st.List(ctx, doc)
	if err != nil {
		t.Fatal(err)
	}
	if ver != 3 || len(got) != 1 || got[0].GetClient() != 2 || got[0].GetVersion() != 2 {
		t.Fatalf("list %+v ver=%d", got, ver)
	}
}

func openStore(t *testing.T, ttl time.Duration) (*store.Store, *redis.Client) {
	t.Helper()
	rdb := testredis.Client(t)
	return store.New(rdb, ttl), rdb
}

func subscribe(t *testing.T, rdb *redis.Client, doc string) <-chan *redis.Message {
	t.Helper()
	sub := rdb.Subscribe(t.Context(), broadcast.Channel(doc))
	t.Cleanup(func() { _ = sub.Close() })
	if _, err := sub.Receive(t.Context()); err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	return sub.Channel()
}

func collectN(t *testing.T, ch <-chan *redis.Message, n int, d time.Duration) []*presencev1.Event {
	t.Helper()
	deadline := time.After(d)
	evs := make([]*presencev1.Event, 0, n)
	for len(evs) < n {
		select {
		case m := <-ch:
			ev, err := broadcast.DecodeEvent([]byte(m.Payload))
			if err != nil {
				t.Fatalf("decode: %v payload=%q", err, m.Payload)
			}
			evs = append(evs, ev)
		case <-deadline:
			t.Fatalf("got %d events, want %d", len(evs), n)
		}
	}
	return evs
}

func assertNoMore(t *testing.T, ch <-chan *redis.Message, d time.Duration) {
	t.Helper()
	select {
	case m := <-ch:
		t.Fatalf("extra event %q", m.Payload)
	case <-time.After(d):
	}
}
