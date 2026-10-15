package hub

import (
	"context"
	"errors"
	"slices"
	"testing"
	"time"

	collabv1 "github.com/rowjay007/collab-sphere/gen/collab/v1"
	"github.com/rowjay007/collab-sphere/pkg/crdt"
)

func chain(client crdt.ClientID, n int) []crdt.Op {
	ops := make([]crdt.Op, n)
	parent := crdt.Root
	for i := range ops {
		op := crdt.Insert{ID: crdt.ID{Client: client, Clock: uint64(i + 1)}, Parent: parent, Value: 'x'}
		ops[i] = op
		parent = op.ID
	}
	return ops
}

func seedLog(t *testing.T, store *MemoryStore, ops []crdt.Op) {
	t.Helper()
	if _, err := store.AppendFromOtherWriter("doc", ops...); err != nil {
		t.Fatal(err)
	}
}

func resumeJoin(t *testing.T, ctx context.Context, reg *Registry, id crdt.ClientID, seq uint64) (*Session, *Peer) {
	t.Helper()
	peer := NewPeer(id)
	session, err := reg.Join(ctx, "doc", peer, true, seq)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { session.Leave(context.Background()) })
	return session, peer
}

// readMissed reads MissedOps frames until one ends at its seq, checking that
// their first_seqs are contiguous from after+1.
func readMissed(t *testing.T, ctx context.Context, peer *Peer, after uint64) (frames []*collabv1.MissedOps, ids []crdt.ID) {
	t.Helper()
	next := after + 1
	for {
		m := readServerMessage(t, ctx, peer).GetMissedOps()
		if m == nil {
			t.Fatal("expected MissedOps")
		}
		if m.GetFirstSeq() != next {
			t.Fatalf("MissedOps first_seq %d, want %d", m.GetFirstSeq(), next)
		}
		frames = append(frames, m)
		for _, pb := range m.GetOperations() {
			op, err := crdt.OpFromProto(pb)
			if err != nil {
				t.Fatal(err)
			}
			ids = append(ids, op.OpID())
		}
		next += uint64(len(m.GetOperations()))
		switch {
		case next-1 == m.GetSeq():
			return frames, ids
		case next-1 > m.GetSeq():
			t.Fatalf("MissedOps reaches seq %d past its seq %d", next-1, m.GetSeq())
		}
	}
}

func opIDs(ops []crdt.Op) []crdt.ID {
	ids := make([]crdt.ID, len(ops))
	for i, op := range ops {
		ids[i] = op.OpID()
	}
	return ids
}

func TestResumeWithinWindowGetsMissedOps(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	store := NewMemoryStore()
	log := chain(50, 5)
	seedLog(t, store, log)
	reg := NewRegistry(store)
	t.Cleanup(reg.Close)

	_, peer := resumeJoin(t, ctx, reg, 1, 2)
	frames, ids := readMissed(t, ctx, peer, 2)
	if len(frames) != 1 || frames[0].GetSeq() != 5 || frames[0].GetDocumentId() != "doc" {
		t.Fatalf("frames = %v", frames)
	}
	if want := opIDs(log[2:]); !slices.Equal(ids, want) {
		t.Fatalf("MissedOps holds %v, want %v", ids, want)
	}

	_, peer = resumeJoin(t, ctx, reg, 2, 5)
	frames, ids = readMissed(t, ctx, peer, 5)
	if len(frames) != 1 || len(ids) != 0 || frames[0].GetSeq() != 5 {
		t.Fatalf("resume at D got %v", frames)
	}
}

func TestResumeBeyondMaxReplayGetsDocumentSync(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	store := NewMemoryStore()
	seedLog(t, store, chain(50, 5))
	reg := NewRegistry(store)
	reg.maxReplay = 3
	t.Cleanup(reg.Close)

	_, peer := resumeJoin(t, ctx, reg, 1, 1)
	sync := readServerMessage(t, ctx, peer).GetDocumentSync()
	if sync == nil || sync.GetSeq() != 5 {
		t.Fatalf("expected DocumentSync at seq 5, got %v", sync)
	}

	_, peer = resumeJoin(t, ctx, reg, 2, 2)
	if _, ids := readMissed(t, ctx, peer, 2); len(ids) != 3 {
		t.Fatalf("resume at the window edge got %d ops, want 3", len(ids))
	}
}

func TestResumeAboveLogIsInvalid(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	store := NewMemoryStore()
	seedLog(t, store, chain(50, 3))
	reg := NewRegistry(store)
	t.Cleanup(reg.Close)

	peer := NewPeer(1)
	if _, err := reg.Join(ctx, "doc", peer, true, 7); !errors.Is(err, ErrInvalidResume) {
		t.Fatalf("Join error = %v", err)
	}
	if code := readServerMessage(t, ctx, peer).GetError().GetCode(); code != collabv1.ErrorMessage_CODE_INVALID_RESUME {
		t.Fatalf("error code = %s", code)
	}
}

func TestResumeCatchesUpBeforeValidating(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	store := NewMemoryStore()
	reg := NewRegistry(store)
	reg.gapGrace = time.Hour
	t.Cleanup(reg.Close)
	_, _, aliceStream := join(t, ctx, reg, 1)

	remote := chain(50, 3)
	seedLog(t, store, remote)
	bob, bobPeer := resumeJoin(t, ctx, reg, 2, 3)
	if _, ids := readMissed(t, ctx, bobPeer, 3); len(ids) != 0 {
		t.Fatalf("bob missed %v, want nothing", ids)
	}
	if err := aliceStream.waitSeq(ctx, 3); err != nil {
		t.Fatal(err)
	}

	reply := crdt.Insert{ID: crdt.ID{Client: 2, Clock: 4}, Parent: remote[2].OpID(), Value: 'y'}
	bob.SubmitOps(ctx, []crdt.Op{reply})
	if err := aliceStream.waitSeq(ctx, 4); err != nil {
		t.Fatal(err)
	}
	if _, got, _, _ := aliceStream.snapshot(); !slices.Equal(got, append(opIDs(remote), reply.ID)) {
		t.Fatalf("alice saw %v", got)
	}
	m := readServerMessage(t, ctx, bobPeer)
	if b := m.GetOperationBroadcast(); b == nil || b.GetFirstSeq() != 4 {
		t.Fatalf("bob's next frame is %v, want his op at seq 4", m)
	}
	select {
	case <-bobPeer.Done():
		t.Fatalf("bob was dropped: %s", bobPeer.DropReason())
	default:
	}
	assertConverged(t, store, aliceStream)
}

func TestResumeCatchUpReadFailureIsUnavailable(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	store := &failingReadStore{MemoryStore: NewMemoryStore()}
	reg := NewRegistry(store)
	reg.gapGrace = time.Hour
	t.Cleanup(reg.Close)
	join(t, ctx, reg, 1)
	seedLog(t, store.MemoryStore, chain(50, 2))

	peer := NewPeer(2)
	if _, err := reg.Join(ctx, "doc", peer, true, 2); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("Join error = %v", err)
	}
	if code := readServerMessage(t, ctx, peer).GetError().GetCode(); code != collabv1.ErrorMessage_CODE_UNAVAILABLE {
		t.Fatalf("error code = %s", code)
	}
}

func TestMissedOpsSplitsIntoFrames(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	const n = 2*missedFrameOps + 100
	store := NewMemoryStore()
	log := chain(50, n)
	seedLog(t, store, log)
	reg := NewRegistry(store)
	t.Cleanup(reg.Close)

	_, peer := resumeJoin(t, ctx, reg, 1, 0)
	frames, ids := readMissed(t, ctx, peer, 0)
	if len(frames) != 3 {
		t.Fatalf("%d frames, want 3", len(frames))
	}
	for i, f := range frames {
		if f.GetSeq() != n {
			t.Fatalf("frame %d carries seq %d, want %d", i, f.GetSeq(), n)
		}
		if len(f.GetOperations()) > missedFrameOps {
			t.Fatalf("frame %d holds %d ops", i, len(f.GetOperations()))
		}
	}
	if !slices.Equal(ids, opIDs(log)) {
		t.Fatal("MissedOps frames do not hold the log in order")
	}
}
