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
	doc, _ := s.Docs.GetDocument(ctx, &documentv1.GetDocumentRequest{Id: docID})
	a := s.dial(ctx, docID, collabclient.Options{URLs: []string{s.HubURL(0)}, Client: 1})
	b := s.dial(ctx, docID, collabclient.Options{URLs: []string{s.HubURL(0)}, Client: 2})
	if err := a.InsertAt(0, 'x'); err != nil {
		t.Fatal(err)
	}
	if err := a.WaitAcked(ctx); err != nil {
		t.Fatal(err)
	}
	if err := a.DeleteAt(0); err != nil {
		t.Fatal(err)
	}
	if err := a.WaitAcked(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Versions.Client.Checkpoint(ctx, &versionv1.CheckpointRequest{
		DocId: docID, Name: "gc-offline", RequestId: t.Name(),
	}); err != nil {
		t.Fatal(err)
	}
	if err := b.InsertAt(0, 'y'); err != nil {
		t.Fatal(err)
	}
	if err := b.WaitAcked(ctx); err != nil {
		t.Fatal(err)
	}
	b.Disconnect()
	waitLiveGC(t, ctx, s, docID)
	opsBefore := operationCount(t, s.dbURL, docID)
	waitLiveGC(t, ctx, s, docID)
	if operationCount(t, s.dbURL, docID) != opsBefore {
		t.Fatal("operations rows changed within TTL")
	}
	_ = doc
}

func TestResyncPastTTLCollectsLeafWithoutAppendingUnsent(t *testing.T) {
	s := New(t, WithExport())
	ctx, cancel := context.WithTimeout(t.Context(), 90*time.Second)
	defer cancel()
	restartDocumentGC(t, s, 30*time.Minute)

	docID := s.CreateDocument(ctx)
	doc, _ := s.Docs.GetDocument(ctx, &documentv1.GetDocumentRequest{Id: docID})
	c := s.dial(ctx, docID, collabclient.Options{URLs: []string{s.HubURL(0)}, Client: 5})
	if err := c.InsertAt(0, 'a'); err != nil {
		t.Fatal(err)
	}
	if err := c.WaitAcked(ctx); err != nil {
		t.Fatal(err)
	}
	if err := c.DeleteAt(0); err != nil {
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
	if err := c.InsertAt(0, 'z'); err != nil {
		t.Fatal(err)
	}
	c.Disconnect()
	ageReplicaReport(t, s.dbURL, doc.GetForkRoot(), 5, 31*time.Minute)
	opsBefore := operationCount(t, s.dbURL, docID)
	waitLiveGC(t, ctx, s, docID)
	if operationCount(t, s.dbURL, docID) != opsBefore {
		t.Fatalf("operations count changed: %d -> %d", opsBefore, operationCount(t, s.dbURL, docID))
	}
	c2 := s.dial(ctx, docID, collabclient.Options{URLs: []string{s.HubURL(0)}, Client: 5})
	if c2.LastJoin() != collabclient.Sync {
		t.Fatalf("LastJoin = %v, want Sync", c2.LastJoin())
	}
	got := ""
	c2.Read(func(r *crdt.Replica) { got, _ = r.Text() })
	if got != "" {
		t.Fatalf("text after resync = %q, want empty", got)
	}
	if err := c2.InsertAt(0, 'q'); err != nil {
		t.Fatal(err)
	}
	time.Sleep(200 * time.Millisecond)
	c2.Disconnect()
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
