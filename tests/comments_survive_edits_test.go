package comments_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"

	commentv1 "github.com/rowjay007/collab-sphere/gen/comment/v1"
	crdtv1 "github.com/rowjay007/collab-sphere/gen/crdt/v1"
	versionv1 "github.com/rowjay007/collab-sphere/gen/version/v1"
	"github.com/rowjay007/collab-sphere/pkg/collabclient"
	"github.com/rowjay007/collab-sphere/pkg/crdt"
	"github.com/rowjay007/collab-sphere/tests/internal/oracle"
	"github.com/rowjay007/collab-sphere/tests/internal/stack"
)

func TestCommentsSurviveEdits(t *testing.T) {
	s := stack.New(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	vs := stack.StartVersionService(s, 0)
	cs := stack.StartCommentService(s, vs)
	docID := s.CreateDocument(ctx)
	home := uuid.MustParse(docID)

	ann := s.Dial(ctx, docID, 1001)
	bob := s.Dial(ctx, docID, 1002)
	carol := s.Dial(ctx, docID, 1003)
	clients := []*collabclient.Client{ann, bob, carol}

	for _, ch := range "abcdefghij" {
		if err := ann.InsertAt(len([]rune(ann.Text())), ch); err != nil {
			t.Fatal(err)
		}
	}
	stack.Settle(t, ctx, s.Docs, docID, clients)

	var start, end crdt.ID
	ann.Read(func(r *crdt.Replica) {
		var err error
		start, err = r.IDAt(2)
		if err != nil {
			t.Fatal(err)
		}
		end, err = r.IDAt(7)
		if err != nil {
			t.Fatal(err)
		}
	})

	created, err := cs.CreateThread(ctx, &commentv1.CreateThreadRequest{
		DocumentId: docID,
		Anchor:     &commentv1.Anchor{Start: opID(start), End: opID(end)},
		Author:     "ann",
		Body:       "tighten this",
		RequestId:  "e2e-create",
	})
	if err != nil {
		t.Fatal(err)
	}

	assertOracle(t, cs, home, clients, start, end, false)

	var wg sync.WaitGroup
	startCh := make(chan struct{})
	for i, c := range clients {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-startCh
			if err := c.InsertAt(4, rune('0'+i)); err != nil {
				t.Error(err)
			}
		}()
	}
	close(startCh)
	wg.Wait()
	stack.Settle(t, ctx, s.Docs, docID, clients)
	assertOracle(t, cs, home, clients, start, end, false)

	if err := ann.DeleteAt(3); err != nil {
		t.Fatal(err)
	}
	if err := bob.DeleteAt(3); err != nil {
		t.Fatal(err)
	}
	stack.Settle(t, ctx, s.Docs, docID, clients)
	assertOracle(t, cs, home, clients, start, end, false)

	for {
		r, err := ann.ResolveAnchor(start, end)
		if err != nil {
			t.Fatal(err)
		}
		if r.Detached {
			break
		}
		if err := ann.Edit(func(rep *crdt.Replica) ([]crdt.Op, error) {
			for i := 0; i < rep.Len(); i++ {
				id, err := rep.IDAt(i)
				if err != nil {
					return nil, err
				}
				if id == r.First {
					op, err := rep.DeleteAt(i)
					return []crdt.Op{op}, err
				}
			}
			return nil, nil
		}); err != nil {
			t.Fatal(err)
		}
		stack.Settle(t, ctx, s.Docs, docID, clients)
	}
	assertOracle(t, cs, home, clients, start, end, true)

	if _, err := cs.Reply(ctx, &commentv1.ReplyRequest{
		ThreadId:  created.GetId(),
		Author:    "carol",
		Body:      "looks good @bob",
		RequestId: "e2e-reply",
	}); err != nil {
		t.Fatal(err)
	}
	notes, err := cs.ListNotifications(ctx, &commentv1.ListNotificationsRequest{User: "bob"})
	if err != nil {
		t.Fatal(err)
	}
	if len(notes.GetNotifications()) != 1 || notes.GetNotifications()[0].GetKind() != commentv1.Notification_KIND_MENTION {
		t.Fatalf("bob notes %+v", notes)
	}

	if _, err := cs.SetResolved(ctx, &commentv1.SetResolvedRequest{
		ThreadId:  created.GetId(),
		Resolved:  true,
		Actor:     "bob",
		RequestId: "e2e-res",
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := cs.SetResolved(ctx, &commentv1.SetResolvedRequest{
		ThreadId:  created.GetId(),
		Resolved:  false,
		Actor:     "bob",
		RequestId: "e2e-unres",
	}); err != nil {
		t.Fatal(err)
	}
	annNotes, err := cs.ListNotifications(ctx, &commentv1.ListNotificationsRequest{User: "ann"})
	if err != nil {
		t.Fatal(err)
	}
	if len(annNotes.GetNotifications()) != 3 {
		t.Fatalf("ann notes %+v", annNotes)
	}
	if annNotes.GetNotifications()[1].GetKind() != commentv1.Notification_KIND_RESOLVED {
		t.Fatalf("resolve kind %+v", annNotes.GetNotifications()[1])
	}
	if annNotes.GetNotifications()[2].GetKind() != commentv1.Notification_KIND_UNRESOLVED {
		t.Fatalf("unresolve kind %+v", annNotes.GetNotifications()[2])
	}
}

func assertOracle(t *testing.T, cs commentv1.CommentServiceClient, home uuid.UUID, clients []*collabclient.Client, start, end crdt.ID, wantDetached bool) {
	t.Helper()
	listed, err := cs.ListThreads(t.Context(), &commentv1.ListThreadsRequest{
		DocumentId:      home.String(),
		IncludeResolved: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(listed.GetThreads()) != 1 {
		t.Fatalf("threads %d", len(listed.GetThreads()))
	}
	server := rangeFromProto(listed.GetThreads()[0].GetResolution())
	for i, c := range clients {
		want, ok := oracle.ResolveRange(oracle.FromSnapshot(c.Snapshot()), start, end)
		if !ok {
			t.Fatalf("client %d oracle lost the anchor", i)
		}
		if want.Detached != wantDetached {
			t.Fatalf("client %d oracle detached=%v, want %v", i, want.Detached, wantDetached)
		}
		got, err := c.ResolveAnchor(start, end)
		if err != nil {
			t.Fatal(err)
		}
		if got != want {
			t.Fatalf("client %d ResolveAnchor=%+v oracle=%+v", i, got, want)
		}
		if server != want {
			t.Fatalf("server resolution=%+v oracle=%+v", server, want)
		}
	}
}

func TestCommentsCrossBranch(t *testing.T) {
	s := stack.New(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	vs := stack.StartVersionService(s, 0)
	cs := stack.StartCommentService(s, vs)

	d := s.CreateDocument(ctx)
	ann := s.DialAllocated(ctx, d, 0)
	for _, ch := range "abcdef" {
		if err := ann.InsertAt(len([]rune(ann.Text())), ch); err != nil {
			t.Fatal(err)
		}
	}
	stack.Settle(t, ctx, s.Docs, d, []*collabclient.Client{ann})
	dStart, dEnd := idRange(t, ann, 0, 2)
	before := createThread(t, ctx, cs, d, dStart, dEnd, "ann", "on D before branch", "cb-d-before")

	v := checkpoint(t, ctx, vs.Client, d, "v")
	br, err := vs.Client.Branch(ctx, &versionv1.BranchRequest{VersionId: v.GetId(), Title: "alt", RequestId: t.Name() + "-branch"})
	if err != nil {
		t.Fatal(err)
	}
	b := br.GetDocumentId()
	bob := s.DialAllocated(ctx, b, 0)
	stack.Settle(t, ctx, s.Docs, b, []*collabclient.Client{bob})
	if !hasThread(t, ctx, cs, b, before.GetId(), true) {
		t.Fatal("thread on D before Branch is not visible on B")
	}

	if err := ann.InsertAt(len([]rune(ann.Text())), 'X'); err != nil {
		t.Fatal(err)
	}
	stack.Settle(t, ctx, s.Docs, d, []*collabclient.Client{ann})
	lateStart, lateEnd := idRange(t, ann, len([]rune(ann.Text()))-1, len([]rune(ann.Text()))-1)
	lateD := createThread(t, ctx, cs, d, lateStart, lateEnd, "ann", "on D after branch", "cb-d-after")

	if err := bob.InsertAt(len([]rune(bob.Text())), 'Y'); err != nil {
		t.Fatal(err)
	}
	stack.Settle(t, ctx, s.Docs, b, []*collabclient.Client{bob})
	bStart, bEnd := idRange(t, bob, len([]rune(bob.Text()))-1, len([]rune(bob.Text()))-1)
	onB := createThread(t, ctx, cs, b, bStart, bEnd, "bob", "on B", "cb-b")

	if hasThread(t, ctx, cs, d, onB.GetId(), true) {
		t.Fatal("thread on B listed on D before Merge")
	}
	if hasThread(t, ctx, cs, b, lateD.GetId(), true) {
		t.Fatal("thread on D after Branch is visible on B")
	}

	if _, err := vs.Client.Merge(ctx, &versionv1.MergeRequest{SourceDocId: b, TargetDocId: d, RequestId: t.Name() + "-merge"}); err != nil {
		t.Fatal(err)
	}
	waitSettled(t, ctx, s, d, []*collabclient.Client{ann})

	listedD := listThreads(t, ctx, cs, d, true)
	merged := findThread(t, listedD, onB.GetId())
	want, ok := oracle.ResolveRange(oracle.FromSnapshot(ann.Snapshot()), bStart, bEnd)
	if !ok {
		t.Fatal("oracle lost the B anchor on D")
	}
	if got := rangeFromProto(merged.GetResolution()); got != want {
		t.Fatalf("D resolution=%+v oracle=%+v", got, want)
	}

	if _, err := cs.SetResolved(ctx, &commentv1.SetResolvedRequest{
		ThreadId:  onB.GetId(),
		Resolved:  true,
		Actor:     "ann",
		RequestId: t.Name() + "-res",
	}); err != nil {
		t.Fatal(err)
	}
	listedB := findThread(t, listThreads(t, ctx, cs, b, true), onB.GetId())
	if !listedB.GetResolved() {
		t.Fatal("B thread resolved on D is open on B")
	}
	if hasThread(t, ctx, cs, b, lateD.GetId(), true) {
		t.Fatal("thread on D after Branch is visible on B after Merge")
	}

	restoreDoc := s.CreateDocument(ctx)
	carol := s.DialAllocated(ctx, restoreDoc, 0)
	for _, ch := range "hello" {
		if err := carol.InsertAt(len([]rune(carol.Text())), ch); err != nil {
			t.Fatal(err)
		}
	}
	stack.Settle(t, ctx, s.Docs, restoreDoc, []*collabclient.Client{carol})
	rStart, rEnd := idRange(t, carol, 1, 3)
	restored := createThread(t, ctx, cs, restoreDoc, rStart, rEnd, "carol", "keep me", "cb-restore")
	wantText := "ell"
	rv := checkpoint(t, ctx, vs.Client, restoreDoc, "keep")
	for i := 0; i < 3; i++ {
		if err := carol.DeleteAt(1); err != nil {
			t.Fatal(err)
		}
	}
	stack.Settle(t, ctx, s.Docs, restoreDoc, []*collabclient.Client{carol})
	detached := findThread(t, listThreads(t, ctx, cs, restoreDoc, true), restored.GetId())
	if detached.GetResolution().GetState() != commentv1.Resolution_STATE_DETACHED {
		t.Fatalf("want DETACHED, got %+v", detached.GetResolution())
	}

	if _, err := vs.Client.Restore(ctx, &versionv1.RestoreRequest{
		DocId: restoreDoc, VersionId: rv.GetId(), RequestId: t.Name() + "-restore",
	}); err != nil {
		t.Fatal(err)
	}
	waitSettled(t, ctx, s, restoreDoc, []*collabclient.Client{carol})
	attached := findThread(t, listThreads(t, ctx, cs, restoreDoc, true), restored.GetId())
	if attached.GetResolution().GetState() != commentv1.Resolution_STATE_ATTACHED {
		t.Fatalf("want ATTACHED after Restore, got %+v", attached.GetResolution())
	}
	if attached.GetResolution().GetText() != wantText {
		t.Fatalf("restored text %q, want %q", attached.GetResolution().GetText(), wantText)
	}
}

func waitSettled(t *testing.T, ctx context.Context, s *stack.Stack, docID string, clients []*collabclient.Client) {
	t.Helper()
	var last string
	for {
		text, _ := stack.LoadText(t, ctx, s.Docs, docID)
		last = text
		ok := true
		for _, c := range clients {
			if c.Text() != text {
				ok = false
				break
			}
		}
		if ok {
			stack.Settle(t, ctx, s.Docs, docID, clients)
			return
		}
		select {
		case <-ctx.Done():
			t.Fatalf("clients did not catch LoadState %q: %v", last, ctx.Err())
		case <-time.After(50 * time.Millisecond):
		}
	}
}

func checkpoint(t *testing.T, ctx context.Context, vs versionv1.VersionServiceClient, docID, name string) *versionv1.Version {
	t.Helper()
	v, err := vs.Checkpoint(ctx, &versionv1.CheckpointRequest{DocId: docID, Name: name, RequestId: t.Name() + "-" + name})
	if err != nil {
		t.Fatal(err)
	}
	return v
}

func createThread(t *testing.T, ctx context.Context, cs commentv1.CommentServiceClient, docID string, start, end crdt.ID, author, body, req string) *commentv1.Thread {
	t.Helper()
	th, err := cs.CreateThread(ctx, &commentv1.CreateThreadRequest{
		DocumentId: docID,
		Anchor:     &commentv1.Anchor{Start: opID(start), End: opID(end)},
		Author:     author,
		Body:       body,
		RequestId:  req,
	})
	if err != nil {
		t.Fatal(err)
	}
	return th
}

func listThreads(t *testing.T, ctx context.Context, cs commentv1.CommentServiceClient, docID string, includeResolved bool) []*commentv1.Thread {
	t.Helper()
	listed, err := cs.ListThreads(ctx, &commentv1.ListThreadsRequest{
		DocumentId:      docID,
		IncludeResolved: includeResolved,
	})
	if err != nil {
		t.Fatal(err)
	}
	return listed.GetThreads()
}

func hasThread(t *testing.T, ctx context.Context, cs commentv1.CommentServiceClient, docID, threadID string, includeResolved bool) bool {
	t.Helper()
	for _, th := range listThreads(t, ctx, cs, docID, includeResolved) {
		if th.GetId() == threadID {
			return true
		}
	}
	return false
}

func findThread(t *testing.T, threads []*commentv1.Thread, id string) *commentv1.Thread {
	t.Helper()
	for _, th := range threads {
		if th.GetId() == id {
			return th
		}
	}
	t.Fatalf("thread %s not listed", id)
	return nil
}

func idRange(t *testing.T, c *collabclient.Client, i, j int) (crdt.ID, crdt.ID) {
	t.Helper()
	var start, end crdt.ID
	c.Read(func(r *crdt.Replica) {
		var err error
		start, err = r.IDAt(i)
		if err != nil {
			t.Fatal(err)
		}
		end, err = r.IDAt(j)
		if err != nil {
			t.Fatal(err)
		}
	})
	return start, end
}

func rangeFromProto(r *commentv1.Resolution) crdt.Range {
	if r.GetState() == commentv1.Resolution_STATE_DETACHED {
		return crdt.Range{Detached: true}
	}
	return crdt.Range{
		First: crdt.ID{Client: crdt.ClientID(r.GetFirst().GetClient()), Clock: r.GetFirst().GetClock()},
		Last:  crdt.ID{Client: crdt.ClientID(r.GetLast().GetClient()), Clock: r.GetLast().GetClock()},
		Text:  r.GetText(),
	}
}

func opID(id crdt.ID) *crdtv1.OpID {
	return &crdtv1.OpID{Client: uint64(id.Client), Clock: id.Clock}
}
