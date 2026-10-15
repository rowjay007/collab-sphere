package store_test

import (
	"context"
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

func upsert(ctx context.Context, st *store.Store, s *presencev1.State) error {
	_, err := st.Join(ctx, s)
	return err
}

func TestUpsertOverwritesAndPublishes(t *testing.T) {
	st, rdb := openStore(t, time.Second)
	ctx := t.Context()
	doc := uuid.NewString()
	ch := subscribe(t, rdb, doc)

	first := &presencev1.State{DocumentId: doc, Client: 7, Session: 11, DisplayName: "ann"}
	if err := upsert(ctx, st, first); err != nil {
		t.Fatalf("upsert: %v", err)
	}
	if first.GetVersion() == 0 {
		t.Fatalf("version %d", first.GetVersion())
	}
	second := &presencev1.State{DocumentId: doc, Client: 7, Session: 99, DisplayName: "bob"}
	if err := upsert(ctx, st, second); err != nil {
		t.Fatalf("rejoin: %v", err)
	}
	if second.GetVersion() <= first.GetVersion() {
		t.Fatalf("version %d", second.GetVersion())
	}

	evs := collectN(t, ch, 2, time.Second)
	if evs[0].GetVersion() != first.GetVersion() || evs[0].GetState().GetSession() != 11 || evs[0].GetState().GetDisplayName() != "ann" {
		t.Fatalf("event 0 %+v", evs[0])
	}
	if evs[1].GetVersion() != second.GetVersion() || evs[1].GetState().GetSession() != 99 || evs[1].GetState().GetDisplayName() != "bob" {
		t.Fatalf("event 1 %+v", evs[1])
	}
	assertNoMore(t, ch, 50*time.Millisecond)

	got, ver, err := st.List(ctx, doc)
	if err != nil {
		t.Fatal(err)
	}
	if ver != second.GetVersion() || len(got) != 1 || got[0].GetSession() != 99 || got[0].GetDisplayName() != "bob" {
		t.Fatalf("list %+v ver=%d", got, ver)
	}
}

func TestJoinRejectsOlderEpochAndOldLeave(t *testing.T) {
	st, rdb := openStore(t, time.Second)
	ctx := t.Context()
	doc := uuid.NewString()
	ch := subscribe(t, rdb, doc)

	old := &presencev1.State{DocumentId: doc, Client: 7, Session: 11, Epoch: 1}
	result, err := st.Join(ctx, old)
	if err != nil || result != store.UpsertApplied {
		t.Fatalf("old join result=%v err=%v", result, err)
	}
	newer := &presencev1.State{DocumentId: doc, Client: 7, Session: 22, Epoch: 2}
	result, err = st.Join(ctx, newer)
	if err != nil || result != store.UpsertApplied {
		t.Fatalf("new join result=%v err=%v", result, err)
	}
	collectN(t, ch, 2, time.Second)

	late := &presencev1.State{DocumentId: doc, Client: 7, Session: 11, Epoch: 1}
	result, err = st.Join(ctx, late)
	if err != nil || result != store.UpsertSuperseded {
		t.Fatalf("late join result=%v err=%v", result, err)
	}
	if err := st.Remove(ctx, &presencev1.Leave{DocumentId: doc, Client: 7, Session: 11}); err != nil {
		t.Fatal(err)
	}
	assertNoMore(t, ch, 50*time.Millisecond)

	got, ver, err := st.List(ctx, doc)
	if err != nil {
		t.Fatal(err)
	}
	if ver != newer.GetVersion() || len(got) != 1 || got[0].GetSession() != 22 || got[0].GetEpoch() != 2 {
		t.Fatalf("list %+v ver=%d", got, ver)
	}
}

func TestApplyUpsertCompareAndSet(t *testing.T) {
	st, rdb := openStore(t, time.Second)
	ctx := t.Context()
	doc := uuid.NewString()
	ch := subscribe(t, rdb, doc)

	owner := &presencev1.State{DocumentId: doc, Client: 7, Session: 22, Epoch: 2}
	if _, err := st.Join(ctx, owner); err != nil {
		t.Fatal(err)
	}
	collectN(t, ch, 1, time.Second)

	stale := &presencev1.State{DocumentId: doc, Client: 7, Session: 11, Epoch: 1}
	same := &presencev1.State{DocumentId: doc, Client: 7, Session: 22, Epoch: 2, DisplayName: "same"}
	absent := &presencev1.State{DocumentId: doc, Client: 8, Session: 11, Epoch: 1}
	_, _, superseded, err := st.Apply(ctx, []*presencev1.Change{
		{Kind: &presencev1.Change_Upsert{Upsert: stale}},
		{Kind: &presencev1.Change_Upsert{Upsert: same}},
		{Kind: &presencev1.Change_Upsert{Upsert: absent}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(superseded) != 1 || superseded[0] != stale {
		t.Fatalf("superseded %+v", superseded)
	}
	events := collectN(t, ch, 2, time.Second)
	if events[0].GetState().GetSession() != 22 || events[1].GetState().GetClient() != 8 {
		t.Fatalf("events %+v", events)
	}

	got, _, err := st.List(ctx, doc)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 {
		t.Fatalf("states %+v", got)
	}
	for _, state := range got {
		if state.GetClient() == 7 && (state.GetSession() != 22 || state.GetDisplayName() != "same") {
			t.Fatalf("owner overwritten %+v", state)
		}
	}
}

func TestTouchOutcomes(t *testing.T) {
	st, rdb := openStore(t, 300*time.Millisecond)
	ctx := t.Context()
	doc := uuid.NewString()
	ch := subscribe(t, rdb, doc)

	self := &presencev1.State{DocumentId: doc, Client: 3, Session: 1 << 60, DisplayName: "ann"}
	if err := upsert(ctx, st, self); err != nil {
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
	if ver != self.GetVersion() || len(got) != 1 || got[0].GetSession() != 1<<60 {
		t.Fatalf("still live %+v ver=%d", got, ver)
	}
}

func TestTouchTreatsOverdueDeadlineAsMissing(t *testing.T) {
	st, rdb := openStore(t, 100*time.Millisecond)
	ctx := t.Context()
	doc := uuid.NewString()
	ch := subscribe(t, rdb, doc)
	self := &presencev1.State{DocumentId: doc, Client: 3, Session: 9}
	if _, err := st.Join(ctx, self); err != nil {
		t.Fatal(err)
	}
	collectN(t, ch, 1, time.Second)
	time.Sleep(150 * time.Millisecond)

	result, err := st.Touch(ctx, &presencev1.Touch{DocumentId: doc, Client: 3, Session: 9})
	if err != nil || result != store.TouchMissing {
		t.Fatalf("touch result=%v err=%v", result, err)
	}
	_, _, superseded, err := st.Apply(ctx, []*presencev1.Change{{
		Kind: &presencev1.Change_Upsert{Upsert: self},
	}})
	if err != nil || len(superseded) != 0 {
		t.Fatalf("re-upsert superseded=%+v err=%v", superseded, err)
	}
	event := collectN(t, ch, 1, time.Second)[0]
	if event.GetState().GetSession() != 9 || event.GetVersion() != self.GetVersion() {
		t.Fatalf("event %+v", event)
	}
}

func TestRemoveMatchingAndStaleSession(t *testing.T) {
	st, rdb := openStore(t, time.Second)
	ctx := t.Context()
	doc := uuid.NewString()
	ch := subscribe(t, rdb, doc)

	if err := upsert(ctx, st, &presencev1.State{DocumentId: doc, Client: 4, Session: 8, DisplayName: "ann"}); err != nil {
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
	if ver == 0 || len(got) != 1 {
		t.Fatalf("stale remove mutated %+v ver=%d", got, ver)
	}

	if err := st.Remove(ctx, &presencev1.Leave{DocumentId: doc, Client: 4, Session: 8}); err != nil {
		t.Fatal(err)
	}
	left := collectN(t, ch, 1, time.Second)[0]
	if left.GetVersion() <= got[0].GetVersion() || left.GetLeft().GetSession() != 8 || left.GetLeft().GetReason() != presencev1.Left_REASON_LEAVE {
		t.Fatalf("leave %+v", left)
	}
	got, ver, err = st.List(ctx, doc)
	if err != nil {
		t.Fatal(err)
	}
	if ver != left.GetVersion() || len(got) != 0 {
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
	if err := upsert(ctx, st, state); err != nil {
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

	if err := upsert(ctx, st, &presencev1.State{DocumentId: doc, Client: 5, Session: 13, DisplayName: "ann"}); err != nil {
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
	if exp.GetVersion() == 0 || exp.GetLeft().GetSession() != 13 || exp.GetLeft().GetReason() != presencev1.Left_REASON_EXPIRED {
		t.Fatalf("expired %+v", exp)
	}
	assertNoMore(t, ch, 80*time.Millisecond)

	got, ver, err := st.List(ctx, doc)
	if err != nil {
		t.Fatal(err)
	}
	if ver != exp.GetVersion() || len(got) != 0 {
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

	if err := upsert(ctx, st, &presencev1.State{DocumentId: doc, Client: client, Session: session}); err != nil {
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
	if err := upsert(ctx, st, &presencev1.State{DocumentId: doc, Client: 1, Session: 1, DisplayName: "ann"}); err != nil {
		t.Fatal(err)
	}
	time.Sleep(400 * time.Millisecond)
	got, ver, err := st.List(ctx, doc)
	if err != nil {
		t.Fatal(err)
	}
	if ver == 0 || len(got) != 0 {
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
	if err := upsert(ctx, st, a); err != nil {
		t.Fatal(err)
	}
	b := &presencev1.State{DocumentId: doc, Client: 2, Session: 2, DisplayName: "bob"}
	if err := upsert(ctx, st, b); err != nil {
		t.Fatal(err)
	}
	if a.GetVersion() == 0 || b.GetVersion() <= a.GetVersion() {
		t.Fatalf("upsert versions %d %d", a.GetVersion(), b.GetVersion())
	}
	if err := st.Remove(ctx, &presencev1.Leave{DocumentId: doc, Client: 1, Session: 1}); err != nil {
		t.Fatal(err)
	}
	got, ver, err := st.List(ctx, doc)
	if err != nil {
		t.Fatal(err)
	}
	if ver <= b.GetVersion() || len(got) != 1 || got[0].GetClient() != 2 || got[0].GetVersion() != b.GetVersion() {
		t.Fatalf("list %+v ver=%d", got, ver)
	}
}

func TestVersionSurvivesCounterLoss(t *testing.T) {
	st, rdb := openStore(t, time.Second)
	ctx := t.Context()
	doc := uuid.NewString()
	first := &presencev1.State{DocumentId: doc, Client: 1, Session: 1}
	second := &presencev1.State{DocumentId: doc, Client: 2, Session: 2}
	if _, err := st.Join(ctx, first); err != nil {
		t.Fatal(err)
	}
	if _, err := st.Join(ctx, second); err != nil {
		t.Fatal(err)
	}
	highest := second.GetVersion()
	if err := rdb.Del(ctx, "presence-ver:"+doc).Err(); err != nil {
		t.Fatal(err)
	}
	time.Sleep(2 * time.Millisecond)
	third := &presencev1.State{DocumentId: doc, Client: 3, Session: 3}
	if _, err := st.Join(ctx, third); err != nil {
		t.Fatal(err)
	}
	if third.GetVersion() <= highest {
		t.Fatalf("version regressed from %d to %d", highest, third.GetVersion())
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
