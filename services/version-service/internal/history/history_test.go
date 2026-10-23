package history_test

import (
	"bytes"
	"context"
	"errors"
	"maps"
	"math/rand/v2"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"google.golang.org/protobuf/proto"

	"github.com/rowjay007/collab-sphere/pkg/crdt"
	"github.com/rowjay007/collab-sphere/services/version-service/internal/dag"
	"github.com/rowjay007/collab-sphere/services/version-service/internal/history"
	"github.com/rowjay007/collab-sphere/services/version-service/internal/store"
	"github.com/rowjay007/collab-sphere/services/version-service/internal/testdb"
)

type fakeDoc struct {
	ops      []crdt.Op
	byBytes  map[string]uint64
	forkOf   *uuid.UUID
	forkSeq  uint64
	forkRoot uuid.UUID
	replica  *crdt.Replica
	claimed  uint64
	updated  time.Time
}

type fakeDocs struct {
	mu           sync.Mutex
	docs         map[uuid.UUID]*fakeDoc
	rng          *rand.Rand
	hijackAppend func(uuid.UUID, []crdt.Op)
}

func newFakeDocs() *fakeDocs {
	return &fakeDocs{docs: map[uuid.UUID]*fakeDoc{}, rng: rand.New(rand.NewPCG(1, 2))}
}

func (f *fakeDocs) create(t *testing.T) uuid.UUID {
	t.Helper()
	f.mu.Lock()
	defer f.mu.Unlock()
	id := uuid.New()
	r, err := crdt.NewReplica(crdt.ClientID(len(f.docs)+1), nil)
	if err != nil {
		t.Fatal(err)
	}
	f.docs[id] = &fakeDoc{replica: r, forkRoot: id, byBytes: map[string]uint64{}, claimed: 10_000, updated: time.Now()}
	return id
}

func (f *fakeDocs) edit(t *testing.T, id uuid.UUID, n int) {
	t.Helper()
	f.mu.Lock()
	defer f.mu.Unlock()
	d := f.docs[id]
	for range n {
		var (
			op  crdt.Op
			err error
		)
		if l := d.replica.Len(); l > 0 && f.rng.IntN(4) == 0 {
			op, err = d.replica.DeleteAt(f.rng.IntN(l))
		} else {
			op, err = d.replica.InsertAt(f.rng.IntN(l+1), rune('a'+f.rng.IntN(26)))
		}
		if err != nil {
			t.Fatal(err)
		}
		d.ops = append(d.ops, op)
		if raw, err := proto.Marshal(crdt.OpToProto(op)); err == nil {
			d.byBytes[string(raw)] = uint64(len(d.ops))
		}
	}
}

func (f *fakeDocs) Get(_ context.Context, id uuid.UUID) (history.Document, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	d, ok := f.docs[id]
	if !ok {
		return history.Document{}, history.ErrNotFound
	}
	return history.Document{ID: id, OpSeq: uint64(len(d.ops)), ForkOf: d.forkOf, ForkSeq: d.forkSeq, ForkRoot: d.forkRoot}, nil
}

func (f *fakeDocs) Read(_ context.Context, id uuid.UUID, after uint64, limit int) ([]crdt.Op, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	d := f.docs[id]
	end := min(uint64(len(d.ops)), after+uint64(limit))
	if after >= end {
		return nil, nil
	}
	return slices.Clone(d.ops[after:end]), nil
}

func (f *fakeDocs) Fork(_ context.Context, src uuid.UUID, seq uint64, _, _ string) (history.Document, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	s := f.docs[src]
	doc := crdt.New()
	for _, op := range s.ops[:seq] {
		if _, err := doc.Apply(op); err != nil {
			return history.Document{}, err
		}
	}
	r, err := crdt.NewReplica(crdt.ClientID(len(f.docs)+1), doc)
	if err != nil {
		return history.Document{}, err
	}
	id := uuid.New()
	byBytes := map[string]uint64{}
	for i, op := range s.ops[:seq] {
		if raw, err := proto.Marshal(crdt.OpToProto(op)); err == nil {
			byBytes[string(raw)] = uint64(i + 1)
		}
	}
	f.docs[id] = &fakeDoc{ops: slices.Clone(s.ops[:seq]), byBytes: byBytes, forkOf: &src, forkSeq: seq, forkRoot: s.forkRoot, replica: r, claimed: 10_000, updated: time.Now()}
	return history.Document{ID: id, OpSeq: seq, ForkOf: &src, ForkSeq: seq, ForkRoot: s.forkRoot}, nil
}

func (f *fakeDocs) LoadState(_ context.Context, id uuid.UUID) (*crdt.Doc, uint64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	d := f.docs[id]
	doc := crdt.New()
	for _, op := range d.ops {
		if _, err := doc.Apply(op); err != nil {
			return nil, 0, err
		}
	}
	return doc, uint64(len(d.ops)), nil
}

func (f *fakeDocs) Append(_ context.Context, id uuid.UUID, ops []crdt.Op) (history.AppendResult, error) {
	if f.hijackAppend != nil {
		f.hijackAppend(id, ops)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	d := f.docs[id]
	if d.byBytes == nil {
		d.byBytes = map[string]uint64{}
	}
	out := history.AppendResult{Seqs: make([]uint64, len(ops))}
	for i, op := range ops {
		raw, err := proto.Marshal(crdt.OpToProto(op))
		if err != nil {
			return history.AppendResult{}, err
		}
		if seq, ok := d.byBytes[string(raw)]; ok {
			out.Seqs[i] = seq
			continue
		}
		if _, err := d.replica.Apply(op); err != nil {
			return history.AppendResult{}, err
		}
		d.ops = append(d.ops, op)
		seq := uint64(len(d.ops))
		d.byBytes[string(raw)] = seq
		out.Seqs[i] = seq
		if out.FirstNewSeq == 0 {
			out.FirstNewSeq = seq
		}
	}
	return out, nil
}

func (f *fakeDocs) ClaimReplica(_ context.Context, id uuid.UUID, client uint64, _ bool) (crdt.ClientID, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	d := f.docs[id]
	if client != 0 {
		return crdt.ClientID(client), nil
	}
	d.claimed++
	return crdt.ClientID(d.claimed), nil
}

func (f *fakeDocs) ListChanged(_ context.Context, since time.Time, afterID string, limit int) ([]uuid.UUID, time.Time, string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	type row struct {
		id uuid.UUID
		at time.Time
	}
	var rows []row
	var after uuid.UUID
	if afterID != "" {
		var err error
		after, err = uuid.Parse(afterID)
		if err != nil {
			return nil, time.Time{}, "", err
		}
	}
	for id, d := range f.docs {
		if afterID != "" {
			if d.updated.Before(since) || d.updated.Equal(since) && bytes.Compare(id[:], after[:]) <= 0 {
				continue
			}
		} else if d.updated.Before(since) {
			continue
		}
		rows = append(rows, row{id, d.updated})
	}
	slices.SortFunc(rows, func(a, b row) int {
		if a.at.Before(b.at) {
			return -1
		}
		if a.at.After(b.at) {
			return 1
		}
		return bytes.Compare(a.id[:], b.id[:])
	})
	if limit > 0 && len(rows) > limit {
		rows = rows[:limit]
	}
	ids := make([]uuid.UUID, len(rows))
	next, nextAfter := since, ""
	for i, r := range rows {
		ids[i] = r.id
		next = r.at
		nextAfter = r.id.String()
	}
	return ids, next, nextAfter, nil
}

func (f *fakeDocs) setUpdated(id uuid.UUID, at time.Time) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.docs[id].updated = at
}

func (f *fakeDocs) inject(id uuid.UUID, op crdt.Op) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	d := f.docs[id]
	if _, err := d.replica.Apply(op); err != nil {
		return err
	}
	d.ops = append(d.ops, op)
	if raw, err := proto.Marshal(crdt.OpToProto(op)); err == nil {
		d.byBytes[string(raw)] = uint64(len(d.ops))
	}
	d.updated = time.Now()
	return nil
}

func (f *fakeDocs) contentAt(t *testing.T, id uuid.UUID, seq uint64) dag.Hash {
	t.Helper()
	f.mu.Lock()
	defer f.mu.Unlock()
	doc := crdt.New()
	for _, op := range f.docs[id].ops[:seq] {
		if _, err := doc.Apply(op); err != nil {
			t.Fatal(err)
		}
	}
	b, err := crdt.CanonicalBytes(doc)
	if err != nil {
		t.Fatal(err)
	}
	return dag.ContentHash(b)
}

func allVersions(t *testing.T, st *store.Store, doc uuid.UUID) []store.Version {
	t.Helper()
	vs, err := st.Versions(context.Background(), doc, -1, 1000)
	if err != nil {
		t.Fatal(err)
	}
	return vs
}

func seqs(vs []store.Version) []uint64 {
	out := make([]uint64, len(vs))
	for i, v := range vs {
		out[i] = v.Node.Seq
	}
	return out
}

func TestConcurrentMaterializeConverges(t *testing.T) {
	st, _ := testdb.Open(t)
	docs := newFakeDocs()
	h := history.New(st, docs)
	ctx := t.Context()
	doc := docs.create(t)
	docs.edit(t, doc, 1050)

	var wg sync.WaitGroup
	errs := make(chan error, 8)
	for range 8 {
		wg.Go(func() {
			if _, err := h.Materialize(ctx, doc); err != nil {
				errs <- err
			}
		})
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}

	vs := allVersions(t, st, doc)
	want := []uint64{0, 100, 200, 300, 400, 500, 600, 700, 800, 900, 1000}
	if got := seqs(vs); !slices.Equal(got, want) {
		t.Fatalf("seqs %v, want %v", got, want)
	}
	var prev []dag.NodeID
	for _, v := range vs {
		n := dag.Node{Doc: doc, Seq: v.Node.Seq, Content: docs.contentAt(t, doc, v.Node.Seq), Parents: prev}
		if n.ID() != v.ID {
			t.Fatalf("node at %d: stored %s, derived %s", v.Node.Seq, v.ID, n.ID())
		}
		wantLabels := []string{history.LabelAuto}
		if v.Node.Seq == 0 {
			wantLabels = []string{history.LabelGenesis}
		}
		if !slices.Equal(v.Meta.Labels, wantLabels) {
			t.Fatalf("labels at %d: %v", v.Node.Seq, v.Meta.Labels)
		}
		prev = []dag.NodeID{v.ID}
	}

	if _, err := h.Materialize(ctx, doc); err != nil {
		t.Fatal(err)
	}
	if again := allVersions(t, st, doc); len(again) != len(vs) {
		t.Fatalf("rerun changed the node count to %d", len(again))
	}
}

func TestForkOfForkChainAndFrontier(t *testing.T) {
	st, _ := testdb.Open(t)
	docs := newFakeDocs()
	h := history.New(st, docs)
	ctx := t.Context()
	d := docs.create(t)
	docs.edit(t, d, 250)
	if _, err := h.Materialize(ctx, d); err != nil {
		t.Fatal(err)
	}

	b, err := docs.Fork(ctx, d, 150, "b", "b")
	if err != nil {
		t.Fatal(err)
	}
	docs.edit(t, b.ID, 120)
	frontier, err := h.Frontier(ctx, b.ID)
	if err != nil {
		t.Fatal(err)
	}
	if want := map[uuid.UUID]uint64{b.ID: 270, d: 150}; !maps.Equal(frontier, want) {
		t.Fatalf("frontier(B) = %v, want %v", frontier, want)
	}

	dv := allVersions(t, st, d)
	if got := seqs(dv); !slices.Equal(got, []uint64{0, 100, 150, 200}) {
		t.Fatalf("D seqs %v", got)
	}
	base := dv[2]
	if !slices.Equal(base.Meta.Labels, []string{history.LabelForkBase}) || !slices.Equal(base.Node.Parents, []dag.NodeID{dv[1].ID}) {
		t.Fatalf("fork base %+v", base)
	}
	if base.Node.Content != docs.contentAt(t, d, 150) {
		t.Fatal("fork base content differs from D@150")
	}
	bv := allVersions(t, st, b.ID)
	if got := seqs(bv); !slices.Equal(got, []uint64{150, 200}) {
		t.Fatalf("B seqs %v", got)
	}
	if !slices.Equal(bv[0].Node.Parents, []dag.NodeID{base.ID}) || bv[0].Node.Content != base.Node.Content ||
		!slices.Equal(bv[0].Meta.Labels, []string{history.LabelFork}) {
		t.Fatalf("B start %+v", bv[0])
	}
	if !slices.Equal(bv[1].Node.Parents, []dag.NodeID{bv[0].ID}) {
		t.Fatal("B@200 does not follow B@150")
	}

	c, err := docs.Fork(ctx, b.ID, 230, "c", "c")
	if err != nil {
		t.Fatal(err)
	}
	docs.edit(t, c.ID, 5)
	frontier, err = h.Frontier(ctx, c.ID)
	if err != nil {
		t.Fatal(err)
	}
	if want := map[uuid.UUID]uint64{c.ID: 235, b.ID: 230, d: 150}; !maps.Equal(frontier, want) {
		t.Fatalf("frontier(C) = %v, want %v", frontier, want)
	}
	if frontier, err = h.Frontier(ctx, d); err != nil || !maps.Equal(frontier, map[uuid.UUID]uint64{d: 250}) {
		t.Fatalf("frontier(D) = %v, %v", frontier, err)
	}
}

func TestCheckpointRefIsImmutable(t *testing.T) {
	st, _ := testdb.Open(t)
	docs := newFakeDocs()
	h := history.New(st, docs)
	ctx := t.Context()
	d := docs.create(t)
	docs.edit(t, d, 130)

	v, err := h.Checkpoint(ctx, d, "draft", "r1")
	if err != nil {
		t.Fatal(err)
	}
	if v.Node.Seq != 130 || !slices.Equal(v.Meta.Labels, []string{history.CheckpointLabel("draft")}) {
		t.Fatalf("checkpoint %d %v", v.Node.Seq, v.Meta.Labels)
	}
	same, err := h.Checkpoint(ctx, d, "draft", "r2")
	if err != nil || same.ID != v.ID {
		t.Fatalf("same node, new request: %v %v", same.ID, err)
	}

	docs.edit(t, d, 1)
	if _, err := h.Checkpoint(ctx, d, "draft", "r3"); !errors.Is(err, history.ErrAlreadyExists) {
		t.Fatalf("different node: %v", err)
	}
	retry, err := h.Checkpoint(ctx, d, "draft", "r1")
	if err != nil || retry.ID != v.ID {
		t.Fatalf("retry of r1: %v %v", retry.ID, err)
	}

	docs.edit(t, d, 69)
	auto, err := h.Checkpoint(ctx, d, "two hundred", "r4")
	if err != nil {
		t.Fatal(err)
	}
	if auto.Node.Seq != 200 || !slices.Equal(auto.Meta.Labels, []string{history.LabelAuto, history.CheckpointLabel("two hundred")}) {
		t.Fatalf("checkpoint on auto node: %d %v", auto.Node.Seq, auto.Meta.Labels)
	}
	if got := seqs(allVersions(t, st, d)); !slices.Equal(got, []uint64{0, 100, 130, 200}) {
		t.Fatalf("seqs %v", got)
	}
	if !slices.Equal(auto.Node.Parents, []dag.NodeID{v.ID}) {
		t.Fatal("auto node after a checkpoint does not follow it")
	}
}
