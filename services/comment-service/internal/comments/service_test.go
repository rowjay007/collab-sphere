package comments_test

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/google/uuid"

	"github.com/rowjay007/collab-sphere/pkg/crdt"
	"github.com/rowjay007/collab-sphere/services/comment-service/internal/comments"
	"github.com/rowjay007/collab-sphere/services/comment-service/internal/comments/testdb"
)

type memoryDocs struct {
	mu     sync.Mutex
	states map[uuid.UUID]docState
}

type docState struct {
	doc *crdt.Doc
	seq uint64
}

func (m *memoryDocs) put(id uuid.UUID, doc *crdt.Doc, seq uint64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.states == nil {
		m.states = map[uuid.UUID]docState{}
	}
	m.states[id] = docState{doc: doc, seq: seq}
}

func (m *memoryDocs) LoadState(ctx context.Context, id uuid.UUID) (*crdt.Doc, uint64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	s, ok := m.states[id]
	if !ok {
		return nil, 0, comments.ErrNotFound
	}
	return s.doc, s.seq, nil
}

type memoryVersions struct {
	mu        sync.Mutex
	frontiers map[uuid.UUID]map[uuid.UUID]uint64
}

func (m *memoryVersions) set(doc uuid.UUID, frontier map[uuid.UUID]uint64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.frontiers == nil {
		m.frontiers = map[uuid.UUID]map[uuid.UUID]uint64{}
	}
	m.frontiers[doc] = frontier
}

func (m *memoryVersions) Frontier(ctx context.Context, doc uuid.UUID) (map[uuid.UUID]uint64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if f, ok := m.frontiers[doc]; ok {
		return f, nil
	}
	return map[uuid.UUID]uint64{}, nil
}

func textDoc(t *testing.T, text string) (*crdt.Doc, []crdt.ID) {
	t.Helper()
	r, err := crdt.NewReplica(7, nil)
	if err != nil {
		t.Fatal(err)
	}
	var ids []crdt.ID
	for _, ch := range text {
		op, err := r.InsertAt(r.Len(), ch)
		if err != nil {
			t.Fatal(err)
		}
		ids = append(ids, op.ID)
	}
	return r.Doc, ids
}

func TestCreateIdempotentAndConflict(t *testing.T) {
	pool := testdb.Open(t)
	docs := &memoryDocs{}
	vers := &memoryVersions{}
	svc := comments.New(pool, docs, vers)
	ctx := t.Context()

	home := uuid.New()
	doc, ids := textDoc(t, "hello")
	docs.put(home, doc, 5)
	anchor := comments.Anchor{Start: ids[0], End: ids[4]}

	first, err := svc.Create(ctx, home, anchor, "ann", "tighten this @bob", "req-1")
	if err != nil {
		t.Fatal(err)
	}
	if first.CreatedSeq != 5 || first.Author != "ann" || len(first.Comments) != 1 {
		t.Fatalf("thread %+v", first)
	}
	if got := first.Comments[0].Mentions; len(got) != 1 || got[0] != "bob" {
		t.Fatalf("mentions %v", got)
	}

	again, err := svc.Create(ctx, home, anchor, "ann", "tighten this @bob", "req-1")
	if err != nil {
		t.Fatal(err)
	}
	if again.ID != first.ID || again.Comments[0].ID != first.Comments[0].ID {
		t.Fatalf("replay ids %s/%s", again.ID, first.ID)
	}
	if _, err := doc.Apply(crdt.Delete{ID: crdt.ID{Client: 7, Clock: 6}, Target: ids[0]}); err != nil {
		t.Fatal(err)
	}
	afterDelete, err := svc.Create(ctx, home, anchor, "ann", "tighten this @bob", "req-1")
	if err != nil || afterDelete.ID != first.ID {
		t.Fatalf("replay after anchor deletion: thread=%s err=%v", afterDelete.ID, err)
	}

	if _, err := svc.Create(ctx, home, anchor, "ann", "different body", "req-1"); !errors.Is(err, comments.ErrAlreadyExists) {
		t.Fatalf("conflict: %v", err)
	}
}

func TestMentionNotifiesAuthorWhenSelfMentioned(t *testing.T) {
	pool := testdb.Open(t)
	docs := &memoryDocs{}
	svc := comments.New(pool, docs, &memoryVersions{})
	ctx := t.Context()
	home := uuid.New()
	doc, ids := textDoc(t, "a")
	docs.put(home, doc, 1)

	if _, err := svc.Create(ctx, home, comments.Anchor{Start: ids[0], End: ids[0]}, "ann", "@ann check", "self-mention"); err != nil {
		t.Fatal(err)
	}
	notes, err := svc.ListNotifications(ctx, "ann", 0, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(notes) != 1 || notes[0].Kind != comments.Mention {
		t.Fatalf("self mention notes %+v", notes)
	}
}

func TestNotificationsAndResolveLWW(t *testing.T) {
	pool := testdb.Open(t)
	docs := &memoryDocs{}
	vers := &memoryVersions{}
	svc := comments.New(pool, docs, vers)
	ctx := t.Context()

	home := uuid.New()
	doc, ids := textDoc(t, "range")
	docs.put(home, doc, 3)
	anchor := comments.Anchor{Start: ids[0], End: ids[4]}

	th, err := svc.Create(ctx, home, anchor, "ann", "see this @bob", "c1")
	if err != nil {
		t.Fatal(err)
	}
	bobNotes, err := svc.ListNotifications(ctx, "bob", 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(bobNotes) != 1 || bobNotes[0].Kind != comments.Mention || bobNotes[0].ThreadID != th.ID {
		t.Fatalf("bob notes %+v", bobNotes)
	}

	if _, err := svc.Reply(ctx, th.ID, "bob", "done", "r1"); err != nil {
		t.Fatal(err)
	}
	annNotes, err := svc.ListNotifications(ctx, "ann", 0, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(annNotes) != 1 || annNotes[0].Kind != comments.ReplyKind {
		t.Fatalf("ann reply notes %+v", annNotes)
	}

	replay, err := svc.Reply(ctx, th.ID, "bob", "done", "r1")
	if err != nil {
		t.Fatal(err)
	}
	again, err := svc.ListNotifications(ctx, "ann", 0, 10)
	if err != nil {
		t.Fatal(err)
	}
	if replay.Body != "done" || len(again) != 1 {
		t.Fatalf("reply replay comment=%s notes=%d", replay.Body, len(again))
	}
	if _, err := svc.Reply(ctx, th.ID, "bob", "other", "r1"); !errors.Is(err, comments.ErrAlreadyExists) {
		t.Fatalf("reply conflict: %v", err)
	}

	resolved, err := svc.SetResolved(ctx, th.ID, true, "bob", "s1")
	if err != nil || !resolved.Resolved {
		t.Fatalf("resolve %v %v", resolved, err)
	}
	annNotes, err = svc.ListNotifications(ctx, "ann", 0, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(annNotes) != 2 || annNotes[1].Kind != comments.ResolvedKind {
		t.Fatalf("resolve notes %+v", annNotes)
	}

	noop, err := svc.SetResolved(ctx, th.ID, true, "carol", "s2")
	if err != nil || !noop.Resolved {
		t.Fatalf("noop %v %v", noop, err)
	}
	annNotes, err = svc.ListNotifications(ctx, "ann", 0, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(annNotes) != 2 {
		t.Fatalf("noop notified %+v", annNotes)
	}

	unresolved, err := svc.SetResolved(ctx, th.ID, false, "bob", "s3")
	if err != nil || unresolved.Resolved {
		t.Fatalf("unresolve %v %v", unresolved, err)
	}
	replayResolve, err := svc.SetResolved(ctx, th.ID, true, "bob", "s1")
	if err != nil || !replayResolve.Resolved {
		t.Fatalf("replay resolve %v %v", replayResolve, err)
	}
	if _, err := svc.SetResolved(ctx, th.ID, false, "bob", "s1"); !errors.Is(err, comments.ErrAlreadyExists) {
		t.Fatalf("resolve conflict: %v", err)
	}
}

func TestListFiltersByFrontier(t *testing.T) {
	pool := testdb.Open(t)
	docs := &memoryDocs{}
	vers := &memoryVersions{}
	svc := comments.New(pool, docs, vers)
	ctx := t.Context()

	home := uuid.New()
	view := uuid.New()
	doc, ids := textDoc(t, "abcd")
	docs.put(home, doc, 10)
	docs.put(view, doc, 10)
	anchor := comments.Anchor{Start: ids[1], End: ids[2]}

	th, err := svc.Create(ctx, home, anchor, "ann", "note", "vis-1")
	if err != nil {
		t.Fatal(err)
	}
	if th.CreatedSeq != 10 {
		t.Fatalf("created_seq %d", th.CreatedSeq)
	}

	other := uuid.New()
	docs.put(other, doc, 5)
	thOther, err := svc.Create(ctx, other, anchor, "ann", "on other", "vis-other")
	if err != nil {
		t.Fatal(err)
	}

	vers.set(view, map[uuid.UUID]uint64{home: 10, other: 5})
	both, err := svc.List(ctx, view, false)
	if err != nil {
		t.Fatal(err)
	}
	if len(both) != 2 {
		t.Fatalf("frontier keys listed %d", len(both))
	}
	seen := map[uuid.UUID]bool{both[0].ID: true, both[1].ID: true}
	if !seen[th.ID] || !seen[thOther.ID] {
		t.Fatalf("frontier keys %+v", both)
	}

	vers.set(view, map[uuid.UUID]uint64{home: 10})
	listed, err := svc.List(ctx, view, false)
	if err != nil {
		t.Fatal(err)
	}
	if len(listed) != 1 || listed[0].ID != th.ID || listed[0].Resolution.State != comments.Attached || listed[0].Resolution.Text != "bc" {
		t.Fatalf("equal frontier %+v", listed)
	}

	vers.set(view, map[uuid.UUID]uint64{home: 9})
	hidden, err := svc.List(ctx, view, false)
	if err != nil {
		t.Fatal(err)
	}
	if len(hidden) != 0 {
		t.Fatalf("below frontier listed %+v", hidden)
	}

	vers.set(view, map[uuid.UUID]uint64{})
	missing, err := svc.List(ctx, view, false)
	if err != nil {
		t.Fatal(err)
	}
	if len(missing) != 0 {
		t.Fatalf("missing home listed %+v", missing)
	}

	if _, err := svc.SetResolved(ctx, th.ID, true, "ann", "vis-res"); err != nil {
		t.Fatal(err)
	}
	vers.set(view, map[uuid.UUID]uint64{home: 10})
	openOnly, err := svc.List(ctx, view, false)
	if err != nil {
		t.Fatal(err)
	}
	if len(openOnly) != 0 {
		t.Fatalf("resolved leaked %+v", openOnly)
	}
	withResolved, err := svc.List(ctx, view, true)
	if err != nil {
		t.Fatal(err)
	}
	if len(withResolved) != 1 || !withResolved[0].Resolved {
		t.Fatalf("include_resolved %+v", withResolved)
	}
}

func TestCreateRejectsInvisibleOrReversedAnchor(t *testing.T) {
	pool := testdb.Open(t)
	docs := &memoryDocs{}
	svc := comments.New(pool, docs, &memoryVersions{})
	ctx := t.Context()
	home := uuid.New()
	doc, ids := textDoc(t, "ab")
	if _, err := doc.Apply(crdt.Delete{ID: crdt.ID{Client: 7, Clock: 3}, Target: ids[0]}); err != nil {
		t.Fatal(err)
	}
	docs.put(home, doc, 3)

	if _, err := svc.Create(ctx, home, comments.Anchor{Start: ids[0], End: ids[1]}, "ann", "x", "bad-1"); !errors.Is(err, comments.ErrInvalidAnchor) {
		t.Fatalf("deleted start: %v", err)
	}
	live, ids2 := textDoc(t, "cd")
	docs.put(home, live, 2)
	if _, err := svc.Create(ctx, home, comments.Anchor{Start: ids2[1], End: ids2[0]}, "ann", "x", "bad-2"); !errors.Is(err, comments.ErrInvalidAnchor) {
		t.Fatalf("reversed: %v", err)
	}
}

func TestMigrateIdempotent(t *testing.T) {
	pool := testdb.Open(t)
	if err := comments.Migrate(t.Context(), pool); err != nil {
		t.Fatal(err)
	}
}
