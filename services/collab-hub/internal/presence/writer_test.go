package presence

import (
	"context"
	"errors"
	"slices"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"

	presencev1 "github.com/rowjay007/collab-sphere/gen/presence/v1"
	"github.com/rowjay007/collab-sphere/pkg/crdt"
)

type fakeService struct {
	mu         sync.Mutex
	joins      []*presencev1.State
	applies    [][]*presencev1.Change
	fail       int
	missing    func(*presencev1.Touch) bool
	superseded func(*presencev1.Touch) bool
	staleJoin  func(*presencev1.State) bool
	staleWrite func(*presencev1.State) bool
	changed    chan struct{}
}

func newFakeService() *fakeService { return &fakeService{changed: make(chan struct{})} }

func (f *fakeService) Join(_ context.Context, in *presencev1.JoinRequest, _ ...grpc.CallOption) (*presencev1.ListResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.joins = append(f.joins, proto.Clone(in.GetSelf()).(*presencev1.State))
	return &presencev1.ListResponse{Version: 1, Superseded: f.staleJoin != nil && f.staleJoin(in.GetSelf())}, nil
}

func (f *fakeService) Apply(_ context.Context, in *presencev1.ApplyRequest, _ ...grpc.CallOption) (*presencev1.ApplyResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	defer func() {
		close(f.changed)
		f.changed = make(chan struct{})
	}()
	if f.fail > 0 {
		f.fail--
		return nil, errors.New("presence-service down")
	}
	f.applies = append(f.applies, in.GetChanges())
	resp := &presencev1.ApplyResponse{}
	for _, ch := range in.GetChanges() {
		if t := ch.GetTouch(); t != nil && f.missing != nil && f.missing(t) {
			resp.Missing = append(resp.Missing, t)
		}
		if t := ch.GetTouch(); t != nil && f.superseded != nil && f.superseded(t) {
			resp.Superseded = append(resp.Superseded, t)
		}
		if u := ch.GetUpsert(); u != nil && f.staleWrite != nil && f.staleWrite(u) {
			resp.SupersededUpserts = append(resp.SupersededUpserts, u)
		}
	}
	return resp, nil
}

func (f *fakeService) List(context.Context, *presencev1.ListRequest, ...grpc.CallOption) (*presencev1.ListResponse, error) {
	return &presencev1.ListResponse{}, nil
}

func (f *fakeService) waitFor(t *testing.T, cond func(applies [][]*presencev1.Change) bool) {
	t.Helper()
	deadline := time.After(5 * time.Second)
	for {
		f.mu.Lock()
		ok, changed := cond(f.applies), f.changed
		f.mu.Unlock()
		if ok {
			return
		}
		select {
		case <-changed:
		case <-deadline:
			t.Fatal("condition never held")
		}
	}
}

func upserts(applies [][]*presencev1.Change) []*presencev1.State {
	var out []*presencev1.State
	for _, changes := range applies {
		for _, ch := range changes {
			if u := ch.GetUpsert(); u != nil {
				out = append(out, u)
			}
		}
	}
	return out
}

func at(clock uint64) crdt.ID { return crdt.ID{Client: 1, Clock: clock} }

func TestWriterHoldsChangesUntilJoinThenCoalesces(t *testing.T) {
	f := newFakeService()
	w := NewWriter(f, 20*time.Millisecond, time.Hour)
	t.Cleanup(w.Close)
	s := Session{Client: 1, Session: 77}

	w.Rename("doc", s, "ann")
	w.Move("doc", Session{Client: 1, Session: 77, Name: "ann"}, at(1), at(1))
	time.Sleep(60 * time.Millisecond)
	f.mu.Lock()
	early := len(f.applies)
	f.mu.Unlock()
	if early != 0 {
		t.Fatalf("%d Applies before Join", early)
	}

	if _, _, err := w.Join(context.Background(), "doc", s); err != nil {
		t.Fatal(err)
	}
	if got := f.joins[0]; got.GetDisplayName() != "ann" || got.GetSession() != 77 {
		t.Fatalf("Join sent %v", got)
	}
	f.waitFor(t, func(a [][]*presencev1.Change) bool {
		u := upserts(a)
		return len(u) > 0 && u[len(u)-1].GetAnchor().GetClock() == 1
	})

	const moves = 200
	for clock := uint64(2); clock <= moves; clock++ {
		w.Move("doc", Session{Client: 1, Session: 77, Name: "ann"}, at(clock), at(clock))
	}
	f.waitFor(t, func(a [][]*presencev1.Change) bool {
		u := upserts(a)
		return u[len(u)-1].GetAnchor().GetClock() == moves
	})
	f.mu.Lock()
	n := len(upserts(f.applies))
	f.mu.Unlock()
	if n > 10 {
		t.Fatalf("%d moves became %d upserts", moves, n)
	}
}

func TestWriterRetriesAndReupsertsMissing(t *testing.T) {
	f := newFakeService()
	f.fail = 2
	f.missing = func(*presencev1.Touch) bool { return true }
	w := NewWriter(f, 10*time.Millisecond, 50*time.Millisecond)
	t.Cleanup(w.Close)
	s := Session{Client: 1, Session: 5, Name: "ann"}
	if _, _, err := w.Join(context.Background(), "doc", s); err != nil {
		t.Fatal(err)
	}
	w.Move("doc", s, at(3), at(4))
	f.waitFor(t, func(a [][]*presencev1.Change) bool {
		var touched, reupserted bool
		for _, changes := range a {
			for _, ch := range changes {
				if ch.GetTouch() != nil {
					touched = true
				}
				if u := ch.GetUpsert(); touched && u != nil && u.GetHead().GetClock() == 4 {
					reupserted = true
				}
			}
		}
		return reupserted
	})
}

func TestWriterReupsertsMissingButForgetsSuperseded(t *testing.T) {
	f := newFakeService()
	f.missing = func(t *presencev1.Touch) bool { return t.GetClient() == 1 }
	f.superseded = func(t *presencev1.Touch) bool { return t.GetClient() == 2 }
	w := NewWriter(f, time.Hour, time.Hour)
	t.Cleanup(w.Close)
	missing := Session{Client: 1, Session: 10}
	superseded := Session{Client: 2, Session: 20}
	if _, _, err := w.Join(context.Background(), "doc", missing); err != nil {
		t.Fatal(err)
	}
	if _, _, err := w.Join(context.Background(), "doc", superseded); err != nil {
		t.Fatal(err)
	}
	if err := w.send(true); err != nil {
		t.Fatal(err)
	}

	w.Move("doc", missing, at(1), at(1))
	w.Move("doc", superseded, at(2), at(2))
	if err := w.send(false); err != nil {
		t.Fatal(err)
	}

	f.mu.Lock()
	defer f.mu.Unlock()
	got := upserts(f.applies)
	if len(got) != 1 || got[0].GetClient() != 1 {
		t.Fatalf("upserts after touch outcomes: %+v", got)
	}
}

func TestWriterCloseFlushesLeaves(t *testing.T) {
	f := newFakeService()
	w := NewWriter(f, time.Hour, time.Hour)
	s := Session{Client: 1, Session: 9}
	if _, _, err := w.Join(context.Background(), "doc", s); err != nil {
		t.Fatal(err)
	}
	w.Leave("doc", s)
	w.Close()
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.applies) != 1 || !slices.ContainsFunc(f.applies[0], func(ch *presencev1.Change) bool {
		return ch.GetLeave().GetSession() == 9
	}) {
		t.Fatalf("Close sent %v", f.applies)
	}
}

func TestWriterForgetsSupersededJoin(t *testing.T) {
	f := newFakeService()
	f.staleJoin = func(st *presencev1.State) bool { return st.GetSession() == 20 }
	w := NewWriter(f, time.Hour, time.Hour)
	stale := Session{Client: 2, Session: 20, Epoch: 1}
	if _, _, err := w.Join(context.Background(), "doc", stale); err != nil {
		t.Fatal(err)
	}
	w.Move("doc", stale, at(1), at(1))
	w.Rename("doc", stale, "late")
	if err := w.send(true); err != nil {
		t.Fatal(err)
	}
	w.Leave("doc", stale)
	w.Close()
	if n := writesFor(f, 20); n != 0 {
		t.Fatalf("%d writes for a session whose Join was superseded", n)
	}
}

func TestWriterForgetsSupersededUpsert(t *testing.T) {
	f := newFakeService()
	f.staleWrite = func(st *presencev1.State) bool { return st.GetSession() == 20 }
	w := NewWriter(f, time.Hour, time.Hour)
	stale := Session{Client: 2, Session: 20, Epoch: 1}
	if _, _, err := w.Join(context.Background(), "doc", stale); err != nil {
		t.Fatal(err)
	}
	w.Move("doc", stale, at(1), at(1))
	if err := w.send(false); err != nil {
		t.Fatal(err)
	}
	w.Move("doc", stale, at(2), at(2))
	if err := w.send(true); err != nil {
		t.Fatal(err)
	}
	w.Leave("doc", stale)
	w.Close()
	if n := writesFor(f, 20); n != 1 {
		t.Fatalf("%d writes for session 20, want only the upsert that was superseded", n)
	}
}

func TestWriterChunksHeartbeats(t *testing.T) {
	f := newFakeService()
	w := NewWriter(f, time.Hour, time.Hour)
	t.Cleanup(w.Close)
	const sessions = 2*maxChanges + 100
	for i := range sessions {
		if _, _, err := w.Join(context.Background(), "doc", Session{Client: crdt.ClientID(i + 1), Session: uint64(i + 1)}); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.send(true); err != nil {
		t.Fatal(err)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	touched := 0
	for _, changes := range f.applies {
		if len(changes) > maxChanges {
			t.Fatalf("one Apply carried %d changes", len(changes))
		}
		touched += len(changes)
	}
	if touched != sessions || len(f.applies) != 3 {
		t.Fatalf("%d touches in %d Applies, want %d in 3", touched, len(f.applies), sessions)
	}
}

func writesFor(f *fakeService, session uint64) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	n := 0
	for _, changes := range f.applies {
		for _, ch := range changes {
			if ch.GetUpsert().GetSession() == session || ch.GetTouch().GetSession() == session || ch.GetLeave().GetSession() == session {
				n++
			}
		}
	}
	return n
}
