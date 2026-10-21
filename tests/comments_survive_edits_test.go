package comments_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"

	commentv1 "github.com/rowjay007/collab-sphere/gen/comment/v1"
	crdtv1 "github.com/rowjay007/collab-sphere/gen/crdt/v1"
	"github.com/rowjay007/collab-sphere/pkg/collabclient"
	"github.com/rowjay007/collab-sphere/pkg/crdt"
	"github.com/rowjay007/collab-sphere/tests/internal/oracle"
	"github.com/rowjay007/collab-sphere/tests/internal/stack"
)

type openFrontier struct{}

func (openFrontier) Frontier(_ context.Context, doc uuid.UUID) (map[uuid.UUID]uint64, error) {
	return map[uuid.UUID]uint64{doc: ^uint64(0)}, nil
}

func TestCommentsSurviveEdits(t *testing.T) {
	s := stack.New(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	cs := stack.StartCommentService(s, openFrontier{})
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
