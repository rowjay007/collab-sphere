package hub

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/rowjay007/collab-sphere/pkg/crdt"
)

func waitReport(t *testing.T, ctx context.Context, store *MemoryStore, docID string, replica crdt.ClientID) StoredReport {
	t.Helper()
	for {
		if rep, ok := store.Report(docID, replica); ok {
			return rep
		}
		select {
		case <-ctx.Done():
			t.Fatalf("report for %d was not stored: %v", replica, ctx.Err())
		case <-time.After(5 * time.Millisecond):
		}
	}
}

func TestFreshJoinStoresVector(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	store := NewMemoryStore()
	op := insert(50, 1)
	if _, err := store.AppendFromOtherWriter("doc", op); err != nil {
		t.Fatal(err)
	}
	reg := localRegistry(store)
	reg.idleSync = time.Hour
	t.Cleanup(reg.Close)
	join(t, ctx, reg, 1)

	rep := waitReport(t, ctx, store, "doc", 1)
	if rep.HomeSeq != 1 || !rep.Vector.Covers(op.ID) || rep.Vector[50] != 1 || len(rep.Vector) != 1 {
		t.Fatalf("report = %+v", rep)
	}
}

func TestResumeStoresNothingUntilReport(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	store := NewMemoryStore()
	op := insert(50, 1)
	if _, err := store.AppendFromOtherWriter("doc", op); err != nil {
		t.Fatal(err)
	}
	reg := localRegistry(store)
	reg.idleSync = time.Hour
	t.Cleanup(reg.Close)
	peer := NewPeer(2, 0)
	session, err := reg.Join(ctx, "doc", peer, true, 1)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { session.Leave(context.Background()) })

	deadline := time.Now().Add(100 * time.Millisecond)
	for time.Now().Before(deadline) {
		if _, ok := store.Report("doc", 2); ok {
			t.Fatal("resume stored a report")
		}
		time.Sleep(5 * time.Millisecond)
	}

	session.SubmitReport(ctx, crdt.VersionVector{50: 1, 9: 4})
	rep := waitReport(t, ctx, store, "doc", 2)
	if rep.HomeSeq != 1 || rep.Vector[50] != 1 || rep.Vector[9] != 0 || len(rep.Vector) != 1 {
		t.Fatalf("report = %+v", rep)
	}
	if !rep.Vector.Covers(op.ID) {
		t.Fatalf("vector %v does not cover %s", rep.Vector, op.ID)
	}
}

func TestReportClipsToDocumentVector(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	store := NewMemoryStore()
	reg := localRegistry(store)
	reg.idleSync = time.Hour
	t.Cleanup(reg.Close)
	session, _, stream := join(t, ctx, reg, 1)
	op := insert(1, 1)
	session.SubmitOps(ctx, []crdt.Op{op})
	if err := stream.waitSeq(ctx, 1); err != nil {
		t.Fatal(err)
	}
	session.SubmitReport(ctx, crdt.VersionVector{1: 100, 9: 50})

	deadline := time.Now().Add(2 * time.Second)
	for {
		rep, ok := store.Report("doc", 1)
		if ok && rep.HomeSeq == 1 && rep.Vector[1] == 1 && len(rep.Vector) == 1 && rep.Vector.Covers(op.ID) {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("report = %+v ok=%v", rep, ok)
		}
		select {
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		case <-time.After(5 * time.Millisecond):
		}
	}
}

func TestReportFollowsPersistedOps(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	base := NewMemoryStore()
	store := &blockingAppendStore{
		Store:        base,
		firstStarted: make(chan struct{}),
		releaseFirst: make(chan struct{}),
	}
	t.Cleanup(func() {
		select {
		case <-store.releaseFirst:
		default:
			close(store.releaseFirst)
		}
	})
	reg := localRegistry(store)
	reg.idleSync = time.Hour
	t.Cleanup(reg.Close)
	session, _, stream := join(t, ctx, reg, 1)

	first := insert(1, 1)
	second := insert(1, 2)
	session.SubmitOps(ctx, []crdt.Op{first})
	select {
	case <-store.firstStarted:
	case <-ctx.Done():
		t.Fatal("append did not start")
	}
	session.SubmitOps(ctx, []crdt.Op{second})
	session.SubmitReport(ctx, crdt.VersionVector{1: 99})
	close(store.releaseFirst)
	if err := stream.waitSeq(ctx, 2); err != nil {
		t.Fatal(err)
	}

	deadline := time.Now().Add(2 * time.Second)
	for {
		rep, ok := base.Report("doc", 1)
		if ok && rep.HomeSeq == 2 && rep.Vector[1] == 2 && rep.Vector.Covers(first.ID) && rep.Vector.Covers(second.ID) {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("report = %+v ok=%v", rep, ok)
		}
		select {
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		case <-time.After(5 * time.Millisecond):
		}
	}
}

type slowReportStore struct {
	*MemoryStore
	entered  chan struct{}
	finished chan struct{}
	once     sync.Once
}

func (s *slowReportStore) ReportVector(ctx context.Context, docID string, replica crdt.ClientID, vector crdt.VersionVector, homeSeq uint64) error {
	first := false
	s.once.Do(func() { first = true })
	if first {
		close(s.entered)
		time.Sleep(500 * time.Millisecond)
		close(s.finished)
	}
	return s.MemoryStore.ReportVector(ctx, docID, replica, vector, homeSeq)
}

func TestReportDoesNotBlockApply(t *testing.T) {
	store := &slowReportStore{
		MemoryStore: NewMemoryStore(),
		entered:     make(chan struct{}),
		finished:    make(chan struct{}),
	}
	reg := localRegistry(store)
	reg.idleSync = time.Hour
	t.Cleanup(reg.Close)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	session, _, stream := join(t, ctx, reg, 1)
	select {
	case <-store.entered:
	case <-ctx.Done():
		t.Fatal("ReportVector did not start")
	}
	select {
	case <-store.finished:
		t.Fatal("ReportVector finished during join")
	default:
	}

	start := time.Now()
	session.SubmitOps(ctx, []crdt.Op{insert(1, 1)})
	echoCtx, echoCancel := context.WithTimeout(ctx, 100*time.Millisecond)
	defer echoCancel()
	if err := stream.waitSeq(echoCtx, 1); err != nil {
		t.Fatalf("echo during blocked ReportVector: %v", err)
	}
	if elapsed := time.Since(start); elapsed >= 100*time.Millisecond {
		t.Fatalf("echo took %s, want under 100ms", elapsed)
	}
	select {
	case <-store.finished:
		t.Fatal("ReportVector finished before the echo")
	default:
	}
}

type gateReportStore struct {
	*MemoryStore
	mu      sync.Mutex
	calls   []crdt.VersionVector
	hold    chan struct{}
	entered chan struct{}
}

func (s *gateReportStore) ReportVector(ctx context.Context, docID string, replica crdt.ClientID, vector crdt.VersionVector, homeSeq uint64) error {
	s.mu.Lock()
	s.calls = append(s.calls, vector.Clone())
	n := len(s.calls)
	s.mu.Unlock()
	if n == 1 {
		close(s.entered)
		select {
		case <-s.hold:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return s.MemoryStore.ReportVector(ctx, docID, replica, vector, homeSeq)
}

func (s *gateReportStore) callCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.calls)
}

func (s *gateReportStore) call(i int) crdt.VersionVector {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.calls[i].Clone()
}

func TestReportCoalescesWhileBlocked(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	base := NewMemoryStore()
	if _, err := base.AppendFromOtherWriter("doc", insert(1, 10)); err != nil {
		t.Fatal(err)
	}
	store := &gateReportStore{
		MemoryStore: base,
		hold:        make(chan struct{}),
		entered:     make(chan struct{}),
	}
	t.Cleanup(func() {
		select {
		case <-store.hold:
		default:
			close(store.hold)
		}
	})
	reg := localRegistry(store)
	reg.idleSync = time.Hour
	t.Cleanup(reg.Close)
	peer := NewPeer(1, 0)
	session, err := reg.Join(ctx, "doc", peer, true, 1)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { session.Leave(context.Background()) })

	session.SubmitReport(ctx, crdt.VersionVector{1: 1})
	select {
	case <-store.entered:
	case <-ctx.Done():
		t.Fatal("ReportVector did not start")
	}
	for clock := uint64(2); clock <= 10; clock++ {
		session.SubmitReport(ctx, crdt.VersionVector{1: clock})
	}
	if got := store.callCount(); got != 1 {
		t.Fatalf("calls while blocked = %d, want 1", got)
	}
	close(store.hold)

	deadline := time.Now().Add(2 * time.Second)
	for store.callCount() < 2 {
		if time.Now().After(deadline) {
			t.Fatalf("calls = %d, want 2", store.callCount())
		}
		time.Sleep(5 * time.Millisecond)
	}
	time.Sleep(50 * time.Millisecond)
	if got := store.callCount(); got != 2 {
		t.Fatalf("calls = %d, want at most 2", got)
	}
	if clock := store.call(1)[1]; clock != 10 {
		t.Fatalf("last vector clock = %d, want 10", clock)
	}
}
