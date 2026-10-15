package hub

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"

	collabv1 "github.com/rowjay007/collab-sphere/gen/collab/v1"
	"github.com/rowjay007/collab-sphere/pkg/crdt"
	"github.com/rowjay007/collab-sphere/services/collab-hub/internal/fanout"
)

// stream follows one peer's frames the way a client would. It fails on the
// first OperationBroadcast whose first_seq is not one past the last seq seen.
type stream struct {
	mu      sync.Mutex
	sync    *collabv1.DocumentSync
	doc     *crdt.Doc
	seq     uint64
	ops     []crdt.ID
	frames  []*collabv1.ServerMessage
	err     error
	changed chan struct{}
	ended   chan struct{}
}

func watch(t *testing.T, ctx context.Context, peer *Peer) *stream {
	t.Helper()
	first := readServerMessage(t, ctx, peer)
	sync := first.GetDocumentSync()
	if sync == nil {
		t.Fatalf("first frame is %T", first.GetPayload())
	}
	doc, err := crdt.FromSnapshot(sync.GetSnapshot())
	if err != nil {
		t.Fatal(err)
	}
	s := &stream{sync: sync, doc: doc, seq: sync.GetSeq(), changed: make(chan struct{}), ended: make(chan struct{})}
	go func() {
		defer close(s.ended)
		for {
			select {
			case frame := <-peer.Frames():
				s.handle(frame)
			case <-peer.Done():
				for {
					select {
					case frame := <-peer.Frames():
						s.handle(frame)
					default:
						return
					}
				}
			case <-ctx.Done():
				return
			}
		}
	}()
	return s
}

func (s *stream) handle(frame []byte) {
	s.mu.Lock()
	defer s.mu.Unlock()
	defer func() {
		close(s.changed)
		s.changed = make(chan struct{})
	}()
	msg := &collabv1.ServerMessage{}
	if err := proto.Unmarshal(frame, msg); err != nil {
		s.err = err
		return
	}
	s.frames = append(s.frames, msg)
	b := msg.GetOperationBroadcast()
	if b == nil || s.err != nil {
		return
	}
	if b.GetFirstSeq() != s.seq+1 {
		s.err = fmt.Errorf("broadcast first_seq %d after seq %d", b.GetFirstSeq(), s.seq)
		return
	}
	for _, pb := range b.GetOperations() {
		op, err := crdt.OpFromProto(pb)
		if err == nil {
			_, err = s.doc.Apply(op)
		}
		if err != nil {
			s.err = err
			return
		}
		s.ops = append(s.ops, op.OpID())
	}
	s.seq += uint64(len(b.GetOperations()))
}

func (s *stream) waitSeq(ctx context.Context, want uint64) error {
	for {
		s.mu.Lock()
		seq, err, changed := s.seq, s.err, s.changed
		s.mu.Unlock()
		if err != nil {
			return err
		}
		if seq >= want {
			return nil
		}
		select {
		case <-changed:
		case <-ctx.Done():
			return fmt.Errorf("seq %d, want %d: %w", seq, want, ctx.Err())
		}
	}
}

func (s *stream) snapshot() (seq uint64, ops []crdt.ID, doc *crdt.Doc, frames []*collabv1.ServerMessage) {
	s.mu.Lock()
	defer s.mu.Unlock()
	doc, err := crdt.FromSnapshot(s.doc.Snapshot())
	if err != nil {
		panic(err)
	}
	return s.seq, slices.Clone(s.ops), doc, slices.Clone(s.frames)
}

// lastError waits for the peer to be dropped and returns the last error frame it got.
func (s *stream) lastError(t *testing.T, ctx context.Context) collabv1.ErrorMessage_Code {
	t.Helper()
	select {
	case <-s.ended:
	case <-ctx.Done():
		t.Fatal("peer was not dropped")
	}
	_, _, _, frames := s.snapshot()
	var code collabv1.ErrorMessage_Code
	for _, f := range frames {
		if e := f.GetError(); e != nil {
			code = e.GetCode()
		}
	}
	return code
}

func insert(client crdt.ClientID, clock uint64) crdt.Insert {
	return crdt.Insert{ID: crdt.ID{Client: client, Clock: clock}, Parent: crdt.Root, Value: rune('a' + clock%26)}
}

func join(t *testing.T, ctx context.Context, reg *Registry, id crdt.ClientID) (*Session, *Peer, *stream) {
	t.Helper()
	peer := NewPeer(id)
	session, err := reg.Join(ctx, "doc", peer)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { session.Leave(context.Background()) })
	return session, peer, watch(t, ctx, peer)
}

func logPrefix(t *testing.T, store Store, through uint64) *crdt.Doc {
	t.Helper()
	b, _, err := store.ReadOperations(context.Background(), "doc", 0, int(through))
	if err != nil {
		t.Fatal(err)
	}
	doc := crdt.New()
	for _, op := range b.Ops {
		if _, err := doc.Apply(op); err != nil {
			t.Fatal(err)
		}
	}
	return doc
}

func assertConverged(t *testing.T, store *MemoryStore, streams ...*stream) {
	t.Helper()
	want := store.Doc("doc").Snapshot()
	for i, s := range streams {
		_, _, doc, _ := s.snapshot()
		if !proto.Equal(doc.Snapshot(), want) {
			t.Fatalf("stream %d diverged from the log: %q vs %q", i, doc.Text(), store.Doc("doc").Text())
		}
	}
}

func TestSeqContiguityAcrossConcurrentClients(t *testing.T) {
	const clients, opsEach = 8, 60
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	store := NewMemoryStore()
	reg := NewRegistry(store)
	t.Cleanup(reg.Close)

	sessions := make([]*Session, clients)
	streams := make([]*stream, clients)
	for i := range clients {
		sessions[i], _, streams[i] = join(t, ctx, reg, crdt.ClientID(i+1))
	}
	var wg sync.WaitGroup
	for i, session := range sessions {
		wg.Add(1)
		go func() {
			defer wg.Done()
			client := crdt.ClientID(i + 1)
			for clock := uint64(1); clock <= opsEach; clock += 2 {
				session.SubmitOps(ctx, []crdt.Op{insert(client, clock), insert(client, clock+1)})
			}
		}()
	}
	wg.Wait()
	for i, s := range streams {
		if err := s.waitSeq(ctx, clients*opsEach); err != nil {
			t.Fatalf("client %d: %v", i+1, err)
		}
	}
	assertConverged(t, store, streams...)
}

func TestGapFromAnotherWriterIsHealed(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	store := NewMemoryStore()
	reg := NewRegistry(store)
	t.Cleanup(reg.Close)
	alice, _, aliceStream := join(t, ctx, reg, 1)
	_, _, bobStream := join(t, ctx, reg, 2)

	alice.SubmitOps(ctx, []crdt.Op{insert(1, 1)})
	if err := aliceStream.waitSeq(ctx, 1); err != nil {
		t.Fatal(err)
	}
	remote := []crdt.Op{insert(50, 1), insert(50, 2), insert(50, 3)}
	if _, err := store.AppendFromOtherWriter("doc", remote...); err != nil {
		t.Fatal(err)
	}
	alice.SubmitOps(ctx, []crdt.Op{insert(1, 2)})

	healCtx, healCancel := context.WithTimeout(ctx, 100*time.Millisecond)
	defer healCancel()
	want := []crdt.ID{insert(1, 1).ID, remote[0].OpID(), remote[1].OpID(), remote[2].OpID(), insert(1, 2).ID}
	for name, s := range map[string]*stream{"alice": aliceStream, "bob": bobStream} {
		if err := s.waitSeq(healCtx, 5); err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if _, got, _, _ := s.snapshot(); !slices.Equal(got, want) {
			t.Fatalf("%s saw %v, want %v", name, got, want)
		}
	}
	assertConverged(t, store, aliceStream, bobStream)
}

func TestDuplicateSignalDeliversSenderInOrder(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	store := NewMemoryStore()
	reg := NewRegistry(store)
	t.Cleanup(reg.Close)
	alice, _, aliceStream := join(t, ctx, reg, 1)

	other, mine := insert(50, 1), insert(1, 1)
	if _, err := store.AppendFromOtherWriter("doc", other, mine); err != nil {
		t.Fatal(err)
	}
	alice.SubmitOps(ctx, []crdt.Op{mine})
	if err := aliceStream.waitSeq(ctx, 2); err != nil {
		t.Fatal(err)
	}
	if _, got, _, _ := aliceStream.snapshot(); !slices.Equal(got, []crdt.ID{other.ID, mine.ID}) {
		t.Fatalf("alice saw %v", got)
	}
	if _, opSeq, _ := store.ReadOperations(ctx, "doc", 0, 10); opSeq != 2 {
		t.Fatalf("log holds %d ops, want 2", opSeq)
	}
	assertConverged(t, store, aliceStream)
}

func TestDocumentSyncFillsGapFirst(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	store := NewMemoryStore()
	reg := NewRegistry(store)
	reg.gapGrace = time.Hour
	t.Cleanup(reg.Close)
	alice, _, aliceStream := join(t, ctx, reg, 1)

	alice.SubmitOps(ctx, []crdt.Op{insert(1, 1)})
	if err := aliceStream.waitSeq(ctx, 1); err != nil {
		t.Fatal(err)
	}
	if _, err := store.AppendFromOtherWriter("doc", insert(50, 1)); err != nil {
		t.Fatal(err)
	}
	alice.SubmitOps(ctx, []crdt.Op{insert(1, 2)})
	for {
		if _, opSeq, _ := store.ReadOperations(ctx, "doc", 0, 10); opSeq == 3 {
			break
		}
		select {
		case <-ctx.Done():
			t.Fatal("alice's second op was never appended")
		case <-time.After(time.Millisecond):
		}
	}
	if seq, _, _, _ := aliceStream.snapshot(); seq != 1 {
		t.Fatalf("gap closed before the join: alice at seq %d", seq)
	}

	_, _, bobStream := join(t, ctx, reg, 2)
	if got := bobStream.sync.GetSeq(); got != 3 {
		t.Fatalf("DocumentSync.seq = %d, want 3", got)
	}
	if want := logPrefix(t, store, 3).Snapshot(); !proto.Equal(bobStream.sync.GetSnapshot(), want) {
		t.Fatal("DocumentSync snapshot differs from log[1..3]")
	}
	if err := aliceStream.waitSeq(ctx, 3); err != nil {
		t.Fatal(err)
	}
	assertConverged(t, store, aliceStream, bobStream)
}

func TestNewestConnectionWins(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	store := NewMemoryStore()
	reg := NewRegistry(store)
	t.Cleanup(reg.Close)
	oldSession, oldPeer, _ := join(t, ctx, reg, 1)
	_, _, bobStream := join(t, ctx, reg, 2)

	newSession, newPeer, newStream := join(t, ctx, reg, 1)
	select {
	case <-oldPeer.Done():
	case <-ctx.Done():
		t.Fatal("old connection was not closed")
	}
	if got := oldPeer.DropReason(); got != "superseded" {
		t.Fatalf("old connection drop reason %q", got)
	}

	newSession.SubmitOps(ctx, []crdt.Op{insert(1, 1)})
	oldSession.Leave(ctx)
	newSession.SubmitOps(ctx, []crdt.Op{insert(1, 2)})
	if err := bobStream.waitSeq(ctx, 2); err != nil {
		t.Fatal(err)
	}
	if err := newStream.waitSeq(ctx, 2); err != nil {
		t.Fatal(err)
	}
	select {
	case <-newPeer.Done():
		t.Fatal("old connection's Leave dropped the new one")
	default:
	}
	_, _, _, frames := bobStream.snapshot()
	for _, f := range frames {
		if p := f.GetPresenceBroadcast(); p != nil && p.GetLeft() != nil {
			t.Fatalf("bob saw client %d leave", p.GetClient())
		}
	}
}

func TestAppendFailureIsRetryable(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	store := NewMemoryStore()
	reg := NewRegistry(store)
	t.Cleanup(reg.Close)
	alice, _, aliceStream := join(t, ctx, reg, 1)

	store.FailAppend(errors.New("disk on fire"))
	alice.SubmitOps(ctx, []crdt.Op{insert(1, 1)})
	if code := aliceStream.lastError(t, ctx); code != collabv1.ErrorMessage_CODE_UNAVAILABLE {
		t.Fatalf("error code = %s", code)
	}
}

type failingReadStore struct {
	*MemoryStore
	mu    sync.Mutex
	reads []time.Time
}

func (s *failingReadStore) ReadOperations(context.Context, string, uint64, int) (fanout.Batch, uint64, error) {
	s.mu.Lock()
	s.reads = append(s.reads, time.Now())
	s.mu.Unlock()
	return fanout.Batch{}, 0, errors.New("document-service down")
}

func TestGapFillFailuresBackOffThenFailStop(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	store := &failingReadStore{MemoryStore: NewMemoryStore()}
	reg := NewRegistry(store)
	t.Cleanup(reg.Close)
	alice, _, aliceStream := join(t, ctx, reg, 1)

	if _, err := store.AppendFromOtherWriter("doc", insert(50, 1)); err != nil {
		t.Fatal(err)
	}
	appended := time.Now()
	alice.SubmitOps(ctx, []crdt.Op{insert(1, 1)})
	if code := aliceStream.lastError(t, ctx); code != collabv1.ErrorMessage_CODE_UNAVAILABLE {
		t.Fatalf("error code = %s", code)
	}

	store.mu.Lock()
	reads := slices.Clone(store.reads)
	store.mu.Unlock()
	if len(reads) != maxFillFailures {
		t.Fatalf("%d reads, want %d", len(reads), maxFillFailures)
	}
	prev := appended
	for i, at := range reads {
		if min := defaultGapGrace << i; at.Sub(prev) < min {
			t.Fatalf("read %d came %s after the previous one, want at least %s", i, at.Sub(prev), min)
		}
		prev = at
	}
}
