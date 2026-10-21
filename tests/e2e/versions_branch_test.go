package e2e

import (
	"bytes"
	"context"
	"math/rand/v2"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	documentv1 "github.com/rowjay007/collab-sphere/gen/document/v1"
	versionv1 "github.com/rowjay007/collab-sphere/gen/version/v1"
	"github.com/rowjay007/collab-sphere/pkg/collabclient"
	"github.com/rowjay007/collab-sphere/pkg/crdt"
	"github.com/rowjay007/collab-sphere/tests/internal/oracle"
	"github.com/rowjay007/collab-sphere/tests/internal/stack"
)

func edit(t *testing.T, ctx context.Context, s *stack.Stack, docID string, clients []*collabclient.Client, seed uint64, n int) {
	t.Helper()
	var wg sync.WaitGroup
	for i, c := range clients {
		wg.Go(func() {
			rng := rand.New(rand.NewPCG(seed, uint64(i)))
			for range n {
				if err := randomEdit(rng, c); err != nil {
					t.Error(err)
					return
				}
			}
		})
	}
	wg.Wait()
	stack.Settle(t, ctx, s.Docs, docID, clients)
}

func frontier(t *testing.T, ctx context.Context, vs versionv1.VersionServiceClient, docID string) map[string]uint64 {
	t.Helper()
	resp, err := vs.Frontier(ctx, &versionv1.FrontierRequest{DocId: docID})
	if err != nil {
		t.Fatal(err)
	}
	out := map[string]uint64{}
	for _, e := range resp.GetEntries() {
		out[e.GetDocumentId()] = e.GetSeq()
	}
	return out
}

func checkpoint(t *testing.T, ctx context.Context, vs versionv1.VersionServiceClient, docID, name string) *versionv1.Version {
	t.Helper()
	v, err := vs.Checkpoint(ctx, &versionv1.CheckpointRequest{DocId: docID, Name: name, RequestId: t.Name() + name})
	if err != nil {
		t.Fatal(err)
	}
	return v
}

func branch(t *testing.T, ctx context.Context, vs versionv1.VersionServiceClient, v *versionv1.Version, title string) *versionv1.BranchResponse {
	t.Helper()
	br, err := vs.Branch(ctx, &versionv1.BranchRequest{VersionId: v.GetId(), Title: title, RequestId: t.Name() + title})
	if err != nil {
		t.Fatal(err)
	}
	return br
}

func headSeq(t *testing.T, ctx context.Context, s *stack.Stack, docID string) uint64 {
	t.Helper()
	doc, err := s.Docs.GetDocument(ctx, &documentv1.GetDocumentRequest{Id: docID})
	if err != nil {
		t.Fatal(err)
	}
	return doc.GetOpSeq()
}

func assertFrontier(t *testing.T, got, want map[string]uint64) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("frontier %v, want %v", got, want)
	}
	for doc, seq := range want {
		if got[doc] != seq {
			t.Fatalf("frontier %v, want %v", got, want)
		}
	}
}

func TestBranchFromCheckpointOnLiveDocument(t *testing.T) {
	s := stack.New(t, stack.WithHubs(2))
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Minute)
	defer cancel()
	vs := stack.StartVersionService(s, time.Second)

	d := s.CreateDocument(ctx)
	dClients := []*collabclient.Client{s.DialAllocated(ctx, d, 0), s.DialAllocated(ctx, d, 1)}
	edit(t, ctx, s, d, dClients, 1, 120)

	var typing sync.WaitGroup
	var stop atomic.Bool
	typing.Go(func() {
		rng := rand.New(rand.NewPCG(5, 5))
		for !stop.Load() {
			if err := randomEdit(rng, dClients[0]); err != nil {
				t.Error(err)
				return
			}
			time.Sleep(2 * time.Millisecond)
		}
	})
	v := checkpoint(t, ctx, vs.Client, d, "v")
	br := branch(t, ctx, vs.Client, v, "alt")
	stop.Store(true)
	typing.Wait()
	b := br.GetDocumentId()
	if !slices.Equal(v.GetLabels(), []string{"checkpoint:v"}) && !slices.Equal(v.GetLabels(), []string{"auto", "checkpoint:v"}) {
		t.Fatalf("checkpoint labels %v", v.GetLabels())
	}

	loaded, err := s.Docs.LoadState(ctx, &documentv1.LoadStateRequest{Id: b})
	if err != nil {
		t.Fatal(err)
	}
	forkState, err := crdt.FromSnapshot(loaded.GetSnapshot())
	if err != nil {
		t.Fatal(err)
	}
	for _, p := range loaded.GetOperations() {
		op, err := crdt.OpFromProto(p)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := forkState.Apply(op); err != nil {
			t.Fatal(err)
		}
	}
	forkBytes, err := crdt.CanonicalBytes(forkState)
	if err != nil {
		t.Fatal(err)
	}
	blob, err := vs.Client.GetBlob(ctx, &versionv1.GetBlobRequest{Hash: v.GetContentHash()})
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(forkBytes, blob.GetBytes()) {
		t.Fatal("fork LoadState differs from the checkpoint blob")
	}

	start := br.GetVersion()
	if start.GetDocId() != b || start.GetSeq() != v.GetSeq() || !bytes.Equal(start.GetContentHash(), v.GetContentHash()) ||
		!slices.EqualFunc(start.GetParents(), [][]byte{v.GetId()}, bytes.Equal) {
		t.Fatalf("fork start %v does not hang off checkpoint %v", start, v)
	}
	if bv := listVersions(t, ctx, vs.Client, b); !bytes.Equal(bv[0].GetId(), start.GetId()) {
		t.Fatal("first listed version of B is not the fork start")
	}

	bClients := []*collabclient.Client{s.DialAllocated(ctx, b, 1)}
	edit(t, ctx, s, b, bClients, 2, 150)
	assertFrontier(t, frontier(t, ctx, vs.Client, b), map[string]uint64{b: headSeq(t, ctx, s, b), d: v.GetSeq()})

	w := checkpoint(t, ctx, vs.Client, b, "w")
	c := branch(t, ctx, vs.Client, w, "alt-of-alt").GetDocumentId()
	edit(t, ctx, s, c, []*collabclient.Client{s.DialAllocated(ctx, c, 0)}, 3, 10)
	assertFrontier(t, frontier(t, ctx, vs.Client, c), map[string]uint64{c: headSeq(t, ctx, s, c), b: w.GetSeq(), d: v.GetSeq()})

	edit(t, ctx, s, d, dClients, 4, 60)
	dHead := headSeq(t, ctx, s, d)
	baseSeq := dHead - 7
	if baseSeq%100 == 0 || baseSeq <= v.GetSeq() {
		t.Fatalf("pick another base seq than %d", baseSeq)
	}
	e, err := s.Docs.ForkDocument(ctx, &documentv1.ForkDocumentRequest{SourceId: d, Seq: baseSeq, Title: "direct", RequestId: t.Name() + "direct"})
	if err != nil {
		t.Fatal(err)
	}
	assertFrontier(t, frontier(t, ctx, vs.Client, e.GetId()), map[string]uint64{e.GetId(): baseSeq, d: baseSeq})
	dVersions := listVersions(t, ctx, vs.Client, d)
	i := slices.IndexFunc(dVersions, func(x *versionv1.Version) bool { return x.GetSeq() == baseSeq })
	if i < 1 || !slices.Equal(dVersions[i].GetLabels(), []string{"fork-base"}) ||
		!slices.EqualFunc(dVersions[i].GetParents(), [][]byte{dVersions[i-1].GetId()}, bytes.Equal) {
		t.Fatalf("no fork-base node at %d following D's previous node: %v", baseSeq, versionSeqs(dVersions))
	}
	eStart := listVersions(t, ctx, vs.Client, e.GetId())[0]
	if !slices.EqualFunc(eStart.GetParents(), [][]byte{dVersions[i].GetId()}, bytes.Equal) {
		t.Fatal("direct fork's start does not hang off the fork-base node")
	}

	for _, doc := range []string{d, b, c, e.GetId()} {
		for _, x := range listVersions(t, ctx, vs.Client, doc) {
			verifyVersion(t, ctx, s.Docs, vs.Client, x)
		}
	}
}

func TestDiffAcrossRealBranch(t *testing.T) {
	s := stack.New(t, stack.WithHubs(2))
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Minute)
	defer cancel()
	vs := stack.StartVersionService(s, 0)

	d := s.CreateDocument(ctx)
	dClients := []*collabclient.Client{s.DialAllocated(ctx, d, 0), s.DialAllocated(ctx, d, 1)}
	edit(t, ctx, s, d, dClients, 10, 90)
	v := checkpoint(t, ctx, vs.Client, d, "v")
	b := branch(t, ctx, vs.Client, v, "b")
	bClients := []*collabclient.Client{s.DialAllocated(ctx, b.GetDocumentId(), 1)}

	var wg sync.WaitGroup
	wg.Go(func() { edit(t, ctx, s, d, dClients, 11, 70) })
	wg.Go(func() { edit(t, ctx, s, b.GetDocumentId(), bClients, 12, 110) })
	wg.Wait()
	d2 := checkpoint(t, ctx, vs.Client, d, "d2")
	b2 := checkpoint(t, ctx, vs.Client, b.GetDocumentId(), "b2")

	versions := map[string]*versionv1.Version{"v": v, "fork start": b.GetVersion(), "d2": d2, "b2": b2}
	states := map[string]oracle.State{}
	for name, x := range versions {
		states[name] = oracleState(t, ctx, s.Docs, x.GetDocId(), x.GetSeq())
	}
	kinds := map[versionv1.Span_Kind]crdt.SpanKind{
		versionv1.Span_KIND_RETAINED: crdt.Retained,
		versionv1.Span_KIND_INSERTED: crdt.Inserted,
		versionv1.Span_KIND_DELETED:  crdt.Deleted,
	}
	diff := func(from, to string) ([]crdt.Span, []crdt.Span) {
		want, err := oracle.Diff(states[from], states[to])
		if err != nil {
			t.Fatal(err)
		}
		resp, err := vs.Client.Diff(ctx, &versionv1.DiffRequest{FromVersionId: versions[from].GetId(), ToVersionId: versions[to].GetId()})
		if err != nil {
			t.Fatal(err)
		}
		var got []crdt.Span
		for _, sp := range resp.GetSpans() {
			got = append(got, crdt.Span{
				Kind:  kinds[sp.GetKind()],
				Text:  sp.GetText(),
				First: crdt.ID{Client: crdt.ClientID(sp.GetFirst().GetClient()), Clock: sp.GetFirst().GetClock()},
				Last:  crdt.ID{Client: crdt.ClientID(sp.GetLast().GetClient()), Clock: sp.GetLast().GetClock()},
			})
		}
		return got, want
	}

	for _, pair := range [][2]string{{"v", "d2"}, {"v", "b2"}, {"d2", "b2"}, {"b2", "d2"}} {
		got, want := diff(pair[0], pair[1])
		if !slices.Equal(got, want) {
			t.Fatalf("Diff(%s, %s):\n got %v\nwant %v", pair[0], pair[1], got, want)
		}
		seen := map[crdt.SpanKind]bool{}
		for _, sp := range got {
			seen[sp.Kind] = true
		}
		if !seen[crdt.Retained] || !seen[crdt.Inserted] || !seen[crdt.Deleted] {
			t.Fatalf("Diff(%s, %s) lacks a span kind: %v", pair[0], pair[1], seen)
		}
	}
	for _, pair := range [][2]string{{"v", "fork start"}, {"d2", "d2"}} {
		got, want := diff(pair[0], pair[1])
		if len(got) != 0 {
			t.Fatalf("Diff(%s, %s) of equal content = %v", pair[0], pair[1], got)
		}
		for _, sp := range want {
			if sp.Kind != crdt.Retained {
				t.Fatalf("oracle finds a change between %s and %s: %v", pair[0], pair[1], sp)
			}
		}
	}
}
