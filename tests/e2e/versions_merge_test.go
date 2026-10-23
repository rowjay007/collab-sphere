package e2e

import (
	"bytes"
	"context"
	"math/rand/v2"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	documentv1 "github.com/rowjay007/collab-sphere/gen/document/v1"
	versionv1 "github.com/rowjay007/collab-sphere/gen/version/v1"
	"github.com/rowjay007/collab-sphere/pkg/collabclient"
	"github.com/rowjay007/collab-sphere/pkg/crdt"
	"github.com/rowjay007/collab-sphere/tests/internal/stack"
)

func TestBranchMergeLive(t *testing.T) {
	s := stack.New(t, stack.WithHubs(2))
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Minute)
	defer cancel()
	vs := stack.StartVersionService(s, 0)

	d := s.CreateDocument(ctx)
	dClients := []*collabclient.Client{s.DialAllocated(ctx, d, 0), s.DialAllocated(ctx, d, 1)}
	edit(t, ctx, s, d, dClients, 21, 125)

	v := checkpoint(t, ctx, vs.Client, d, "v")
	br := branch(t, ctx, vs.Client, v, "alt")
	b := br.GetDocumentId()

	xD := s.DialAllocated(ctx, d, 0)
	xB := s.DialAllocated(ctx, b, 1)
	if xD.ID() == 0 || xD.ID() == xB.ID() {
		t.Fatalf("device X ids D=%d B=%d", xD.ID(), xB.ID())
	}
	_, err := collabclient.DialWith(ctx, b, collabclient.Options{URLs: s.HubURLs(), Client: xD.ID()})
	if err == nil || !strings.Contains(err.Error(), "CODE_UNKNOWN_REPLICA") {
		t.Fatalf("D replica on B: %v", err)
	}

	bClients := []*collabclient.Client{s.DialAllocated(ctx, b, 1), xB}
	var wg sync.WaitGroup
	wg.Go(func() { edit(t, ctx, s, d, dClients, 22, 40) })
	wg.Go(func() { edit(t, ctx, s, b, bClients, 23, 40) })
	wg.Wait()
	stack.Settle(t, ctx, s.Docs, b, bClients)

	before := frontier(t, ctx, vs.Client, d)
	if _, ok := before[b]; ok {
		t.Fatalf("frontier before merge includes B: %v", before)
	}

	var typing sync.WaitGroup
	var stop atomic.Bool
	typing.Go(func() {
		rng := rand.New(rand.NewPCG(24, 24))
		for !stop.Load() {
			if err := randomEdit(rng, dClients[0]); err != nil {
				t.Error(err)
				return
			}
			time.Sleep(2 * time.Millisecond)
		}
	})

	const rid = "merge-live"
	vs.Kill()
	vs.Env = []string{"VERSION_CRASH_AT=after_append"}
	vs.Start()
	_, err = vs.Client.Merge(ctx, &versionv1.MergeRequest{SourceDocId: b, TargetDocId: d, RequestId: rid})
	if err == nil {
		t.Fatal("merge succeeded through the crash hook")
	}
	vs.Env = nil
	vs.Start()
	first, err := vs.Client.Merge(ctx, &versionv1.MergeRequest{SourceDocId: b, TargetDocId: d, RequestId: rid})
	if err != nil {
		t.Fatal(err)
	}
	stop.Store(true)
	typing.Wait()
	dClients = append(dClients, xD)
	got, meta := waitConverged(t, ctx, s, d, dClients)

	oracle := mergeOracle(t, ctx, vs.Client, v, dClients, bClients)
	if got != oracle {
		t.Fatalf("LoadState text != oracle\n got %q\nwant %q", got, oracle)
	}

	seq := meta.GetOpSeq()
	again, err := vs.Client.Merge(ctx, &versionv1.MergeRequest{SourceDocId: b, TargetDocId: d, RequestId: rid + "-2"})
	if err != nil {
		t.Fatal(err)
	}
	if !again.GetNoop() {
		t.Fatal("second merge is not noop")
	}
	if headSeq(t, ctx, s, d) != seq {
		t.Fatalf("op_seq changed on noop merge: %d -> %d", seq, headSeq(t, ctx, s, d))
	}

	hb := headSeq(t, ctx, s, b)
	bHead := nodeAtSeq(t, ctx, vs.Client, b, hb)
	mergeV := first.GetVersion()
	if len(mergeV.GetParents()) != 2 {
		t.Fatalf("merge parents %v", mergeV.GetParents())
	}
	dVers := listVersions(t, ctx, vs.Client, d)
	i := indexVersion(dVers, mergeV.GetId())
	if i < 1 || !bytes.Equal(mergeV.GetParents()[0], dVers[i-1].GetId()) || !bytes.Equal(mergeV.GetParents()[1], bHead.GetId()) {
		t.Fatalf("merge parents want [D prev, B@%d]", hb)
	}

	after := frontier(t, ctx, vs.Client, d)
	if after[b] != hb {
		t.Fatalf("frontier after merge: %v, want B@%d", after, hb)
	}

	for _, doc := range []string{d, b} {
		for _, x := range listVersions(t, ctx, vs.Client, doc) {
			verifyVersion(t, ctx, s.Docs, vs.Client, x)
		}
	}
	if first.GetVersion().GetSeq() != again.GetVersion().GetSeq() || !bytes.Equal(first.GetVersion().GetId(), again.GetVersion().GetId()) {
		t.Fatal("crash resume and live merge disagree")
	}
}

func TestRestoreLive(t *testing.T) {
	s := stack.New(t, stack.WithHubs(2))
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Minute)
	defer cancel()
	vs := stack.StartVersionService(s, 0)

	d := s.CreateDocument(ctx)
	clients := []*collabclient.Client{s.DialAllocated(ctx, d, 0), s.DialAllocated(ctx, d, 1)}
	edit(t, ctx, s, d, clients, 31, 40)
	v := checkpoint(t, ctx, vs.Client, d, "v")
	vText := clients[0].Text()
	before := listVersions(t, ctx, vs.Client, d)

	wiper := clients[0]
	for wiper.Text() != "" {
		if err := wiper.DeleteAt(0); err != nil {
			t.Fatal(err)
		}
	}
	if err := wiper.InsertAt(0, 'Z'); err != nil {
		t.Fatal(err)
	}
	if err := clients[1].InsertAt(0, 'Y'); err != nil {
		t.Fatal(err)
	}
	if stack.Settle(t, ctx, s.Docs, d, clients) == vText {
		t.Fatal("edits after V left the text unchanged")
	}

	r, err := vs.Client.Restore(ctx, &versionv1.RestoreRequest{DocId: d, VersionId: v.GetId(), RequestId: t.Name()})
	if err != nil {
		t.Fatal(err)
	}
	if got, _ := stack.LoadText(t, ctx, s.Docs, d); got != vText {
		t.Fatalf("restored text %q, want %q", got, vText)
	}
	waitText(t, ctx, clients, vText)
	stack.Settle(t, ctx, s.Docs, d, clients)

	after := listVersions(t, ctx, vs.Client, d)
	if len(after) < len(before)+1 {
		t.Fatalf("nodes vanished: before %v after %v", versionSeqs(before), versionSeqs(after))
	}
	for _, old := range before {
		if indexVersion(after, old.GetId()) < 0 {
			t.Fatalf("earlier node %d disappeared", old.GetSeq())
		}
	}
	if !bytes.Equal(r.GetRestoredFrom(), v.GetId()) {
		t.Fatalf("restored_from %x", r.GetRestoredFrom())
	}

	diff, err := vs.Client.Diff(ctx, &versionv1.DiffRequest{FromVersionId: v.GetId(), ToVersionId: r.GetId()})
	if err != nil {
		t.Fatal(err)
	}
	for _, sp := range diff.GetSpans() {
		if sp.GetKind() == versionv1.Span_KIND_INSERTED || sp.GetKind() == versionv1.Span_KIND_DELETED {
			t.Fatalf("Diff(V, restore) has %v", sp)
		}
	}

	textAfter := clients[0].Text()
	if _, err := vs.Client.Restore(ctx, &versionv1.RestoreRequest{DocId: d, VersionId: v.GetId(), RequestId: t.Name() + "-2"}); err != nil {
		t.Fatal(err)
	}
	waitText(t, ctx, clients, textAfter)
	stack.Settle(t, ctx, s.Docs, d, clients)
	if got, _ := stack.LoadText(t, ctx, s.Docs, d); got != textAfter {
		t.Fatalf("second restore changed text to %q", got)
	}
}

func waitText(t *testing.T, ctx context.Context, clients []*collabclient.Client, want string) {
	t.Helper()
	for i, c := range clients {
		if err := c.WaitUntil(ctx, func(r *crdt.Replica) bool { return r.Text() == want }); err != nil {
			t.Fatalf("client %d text %q, want %q: %v", i, c.Text(), want, err)
		}
	}
}

func waitConverged(t *testing.T, ctx context.Context, s *stack.Stack, docID string, clients []*collabclient.Client) (string, *documentv1.Document) {
	t.Helper()
	for _, c := range clients {
		if err := c.WaitAcked(ctx); err != nil {
			t.Fatalf("wait acked: %v", err)
		}
	}
	var last string
	for {
		text, meta := stack.LoadText(t, ctx, s.Docs, docID)
		last = text
		ok := true
		for _, c := range clients {
			if c.Text() != text {
				ok = false
				break
			}
		}
		if ok {
			return stack.Settle(t, ctx, s.Docs, docID, clients), meta
		}
		select {
		case <-ctx.Done():
			t.Fatalf("clients did not converge to LoadState %q: %v", last, ctx.Err())
		case <-time.After(50 * time.Millisecond):
		}
	}
}

func authored(c *collabclient.Client) []crdt.Op {
	id := c.ID()
	var ops []crdt.Op
	c.Read(func(r *crdt.Replica) {
		for _, op := range r.Ops() {
			if op.OpID().Client == id {
				ops = append(ops, op)
			}
		}
	})
	return ops
}

func mergeOracle(t *testing.T, ctx context.Context, vs versionv1.VersionServiceClient, v *versionv1.Version, dClients, bClients []*collabclient.Client) string {
	t.Helper()
	blob, err := vs.GetBlob(ctx, &versionv1.GetBlobRequest{Hash: v.GetContentHash()})
	if err != nil {
		t.Fatal(err)
	}
	doc, err := crdt.FromCanonical(blob.GetBytes())
	if err != nil {
		t.Fatal(err)
	}
	for _, group := range [][]*collabclient.Client{dClients, bClients} {
		for _, c := range group {
			for _, op := range authored(c) {
				if doc.Contains(op.OpID()) {
					continue
				}
				if _, err := doc.Apply(op); err != nil {
					t.Fatal(err)
				}
			}
		}
	}
	return doc.Text()
}

func nodeAtSeq(t *testing.T, ctx context.Context, vs versionv1.VersionServiceClient, docID string, seq uint64) *versionv1.Version {
	t.Helper()
	for _, v := range listVersions(t, ctx, vs, docID) {
		if v.GetSeq() == seq {
			return v
		}
	}
	t.Fatalf("no node of %s at %d", docID, seq)
	return nil
}

func indexVersion(vs []*versionv1.Version, id []byte) int {
	for i, v := range vs {
		if bytes.Equal(v.GetId(), id) {
			return i
		}
	}
	return -1
}
