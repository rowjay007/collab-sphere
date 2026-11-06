package stack

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	documentv1 "github.com/rowjay007/collab-sphere/gen/document/v1"
	versionv1 "github.com/rowjay007/collab-sphere/gen/version/v1"
	"github.com/rowjay007/collab-sphere/pkg/collabclient"
	"github.com/rowjay007/collab-sphere/pkg/crdt"
)

func restartDocumentGC(t *testing.T, s *Stack, ttl time.Duration) string {
	t.Helper()
	s.StopDocumentService()
	env := append([]string{
		"DATABASE_URL=" + s.dbURL,
		"GRPC_ADDR=" + s.grpc,
		"GC_INTERVAL=200ms",
		"REPLICA_REPORT_TTL=" + ttl.String(),
	}, s.authEnv()...)
	var commentAddr string
	if s.Versions != nil {
		commentAddr = freeAddr(s.t)
		startCommentOnAddr(s, s.Versions, commentAddr)
		env = append(env, "VERSION_ADDR="+s.Versions.addr, "COMMENT_ADDR="+commentAddr)
	}
	s.docsvc = start(t, "document-service", binary(t, "document-service"), env...)
	waitFor(t, "document-service", func(ctx context.Context) error {
		_, err := s.Docs.GetDocument(ctx, &documentv1.GetDocumentRequest{Id: "00000000-0000-0000-0000-000000000001"})
		if status.Code(err) == codes.NotFound {
			return nil
		}
		return err
	})
	return commentAddr
}

func ageReplicaReport(t *testing.T, dbURL, forkRoot string, replica uint64, by time.Duration) {
	t.Helper()
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, dbURL)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close(ctx)
	if _, err := conn.Exec(ctx, `
		UPDATE replica_reports
		SET reported_at = now() - $3::interval
		WHERE fork_root = $1::uuid AND replica_id = $2::numeric`,
		forkRoot, replica, by.String()); err != nil {
		t.Fatal(err)
	}
}

func operationCount(t *testing.T, dbURL, docID string) int {
	t.Helper()
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, dbURL)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close(ctx)
	var n int
	if err := conn.QueryRow(ctx, `SELECT count(*) FROM operations WHERE document_id = $1::uuid`, docID).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

func waitLiveGC(t *testing.T, ctx context.Context, s *Stack, docID string) {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		resp, err := s.Docs.ReadOperations(ctx, &documentv1.ReadOperationsRequest{Id: docID, AfterSeq: 0, Limit: 1})
		if err == nil && resp.GetLiveSeq() > 0 {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		case <-time.After(200 * time.Millisecond):
		}
	}
	t.Fatal("live snapshot never appeared")
}

func TestGCOfflineReplicaWithinTTLPinsStable(t *testing.T) {
	s := New(t, WithExport())
	ctx, cancel := context.WithTimeout(t.Context(), 60*time.Second)
	defer cancel()
	restartDocumentGC(t, s, 2*time.Hour)

	docID := s.CreateDocument(ctx)
	doc, err := s.Docs.GetDocument(ctx, &documentv1.GetDocumentRequest{Id: docID})
	if err != nil {
		t.Fatal(err)
	}
	a := s.dial(ctx, docID, collabclient.Options{URLs: []string{s.HubURL(0)}, Client: 1})
	insertDeleteAcked := func(r rune) crdt.ID {
		t.Helper()
		if err := a.InsertAt(0, r); err != nil {
			t.Fatal(err)
		}
		var id crdt.ID
		a.Read(func(rep *crdt.Replica) {
			var err error
			if id, err = rep.IDAt(0); err != nil {
				t.Fatal(err)
			}
		})
		if err := a.DeleteAt(0); err != nil {
			t.Fatal(err)
		}
		if err := a.WaitAcked(ctx); err != nil {
			t.Fatal(err)
		}
		return id
	}
	covered := insertDeleteAcked('w')
	b := s.dial(ctx, docID, collabclient.Options{URLs: []string{s.HubURL(0)}, Client: 2})
	waitForkReport(t, ctx, s, doc.GetForkRoot(), 2)
	b.Disconnect()
	uncovered := insertDeleteAcked('x')
	if _, err := s.Versions.Client.Checkpoint(ctx, &versionv1.CheckpointRequest{
		DocId: docID, Name: "gc-offline", RequestId: t.Name(),
	}); err != nil {
		t.Fatal(err)
	}
	ageAllButTipVersion(t, s.Versions.DB(), docID)
	a.Disconnect()
	ageReplicaReport(t, s.dbURL, doc.GetForkRoot(), 1, 3*time.Hour)

	waitLiveState(t, ctx, s, docID, func(*crdt.Doc) bool {
		return !liveDoc(t, s.dbURL, docID).Has(covered)
	})
	if !liveDoc(t, s.dbURL, docID).Has(uncovered) {
		t.Fatal("leaf the offline replica has not covered was collected within TTL")
	}
}

func TestResyncPastTTLCollectsLeafWithoutAppendingUnsent(t *testing.T) {
	s := New(t, WithExport())
	ctx, cancel := context.WithTimeout(t.Context(), 90*time.Second)
	defer cancel()
	restartDocumentGC(t, s, 30*time.Minute)

	docID := s.CreateDocument(ctx)
	doc, err := s.Docs.GetDocument(ctx, &documentv1.GetDocumentRequest{Id: docID})
	if err != nil {
		t.Fatal(err)
	}
	c := s.dial(ctx, docID, collabclient.Options{URLs: []string{s.HubURL(0)}, Client: 5})
	var leaf crdt.ID
	if err := c.Edit(func(r *crdt.Replica) ([]crdt.Op, error) {
		ins, err := r.InsertAfter(crdt.Root, 'a')
		if err != nil {
			return nil, err
		}
		del, err := r.Delete(ins.ID)
		if err != nil {
			return nil, err
		}
		leaf = ins.ID
		return []crdt.Op{ins, del}, nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := c.WaitAcked(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Versions.Client.Checkpoint(ctx, &versionv1.CheckpointRequest{
		DocId: docID, Name: "gc-resync", RequestId: t.Name(),
	}); err != nil {
		t.Fatal(err)
	}
	ageAllButTipVersion(t, s.Versions.DB(), docID)
	c.Disconnect()
	var unsent crdt.ID
	if err := c.Edit(func(r *crdt.Replica) ([]crdt.Op, error) {
		ins, err := r.InsertAfter(leaf, 'z')
		if err != nil {
			return nil, err
		}
		unsent = ins.ID
		return []crdt.Op{ins}, nil
	}); err != nil {
		t.Fatal(err)
	}
	ageReplicaReport(t, s.dbURL, doc.GetForkRoot(), 5, 31*time.Minute)
	opsBefore := operationCount(t, s.dbURL, docID)

	waitLiveState(t, ctx, s, docID, func(*crdt.Doc) bool {
		return !liveDoc(t, s.dbURL, docID).Has(leaf)
	})
	if n := operationCount(t, s.dbURL, docID); n != opsBefore {
		t.Fatalf("operations count changed: %d -> %d", opsBefore, n)
	}

	if err := c.Reconnect(ctx, s.HubURL(0)); err != nil {
		t.Fatal(err)
	}
	if c.LastJoin() != collabclient.Sync {
		t.Fatalf("LastJoin = %v, want Sync", c.LastJoin())
	}
	if c.Unacked() != 0 {
		t.Fatalf("resync kept %d unacked ops", c.Unacked())
	}
	if text := c.Text(); text != "" {
		t.Fatalf("text after resync = %q, want empty", text)
	}
	time.Sleep(500 * time.Millisecond)
	resp, err := s.Docs.ReadOperations(ctx, &documentv1.ReadOperationsRequest{Id: docID, AfterSeq: 0, Limit: 1000})
	if err != nil {
		t.Fatal(err)
	}
	for _, p := range resp.GetOperations() {
		op, err := crdt.OpFromProto(p)
		if err != nil {
			t.Fatal(err)
		}
		if op.OpID() == unsent {
			t.Fatalf("unsent insert %v parented on collected leaf was appended", unsent)
		}
	}
	if n := operationCount(t, s.dbURL, docID); n != opsBefore {
		t.Fatalf("operations count changed after reconnect: %d -> %d", opsBefore, n)
	}
}

func TestGCMergeArchivedBranchFailedPrecondition(t *testing.T) {
	s := New(t, WithExport())
	ctx, cancel := context.WithTimeout(t.Context(), 60*time.Second)
	defer cancel()
	ctx = s.AuthCtx(ctx, s.Owner())
	docID := s.CreateDocument(ctx)
	v, err := s.Versions.Client.Checkpoint(ctx, &versionv1.CheckpointRequest{
		DocId: docID, Name: "head", RequestId: t.Name() + "-cp",
	})
	if err != nil {
		t.Fatal(err)
	}
	branch, err := s.Versions.Client.Branch(ctx, &versionv1.BranchRequest{
		VersionId: v.GetId(), Title: "b", RequestId: t.Name() + "-fork",
	})
	if err != nil {
		t.Fatal(err)
	}
	bID := branch.GetDocumentId()
	if _, err := s.Docs.ArchiveDocument(ctx, &documentv1.ArchiveDocumentRequest{Id: bID}); err != nil {
		t.Fatal(err)
	}
	_, err = s.Versions.Client.Merge(ctx, &versionv1.MergeRequest{
		SourceDocId: bID, TargetDocId: docID, RequestId: t.Name() + "-merge",
	})
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("merge archived branch = %v", err)
	}
}
