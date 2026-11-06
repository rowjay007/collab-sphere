package stack

import (
	"context"
	"testing"
	"time"

	documentv1 "github.com/rowjay007/collab-sphere/gen/document/v1"
	versionv1 "github.com/rowjay007/collab-sphere/gen/version/v1"
	"github.com/rowjay007/collab-sphere/pkg/collabclient"
	"github.com/rowjay007/collab-sphere/pkg/crdt"
)

func TestGCLiveSnapshotStack(t *testing.T) {
	s := New(t, WithExport())
	ctx, cancel := context.WithTimeout(t.Context(), 45*time.Second)
	defer cancel()
	commentAddr := freeAddr(s.t)
	startCommentOnAddr(s, s.Versions, commentAddr)
	s.docsvc.kill()
	s.docsvc = start(s.t, "document-service", binary(s.t, "document-service"), append([]string{
		"DATABASE_URL=" + s.dbURL,
		"GRPC_ADDR=" + s.grpc,
		"VERSION_ADDR=" + s.Versions.addr,
		"COMMENT_ADDR=" + commentAddr,
		"GC_INTERVAL=200ms",
		"REPLICA_REPORT_TTL=336h",
	}, s.authEnv()...)...)
	waitFor(s.t, "document-service", func(ctx context.Context) error {
		_, err := s.Docs.CreateDocument(s.AuthCtx(ctx, s.Owner()), &documentv1.CreateDocumentRequest{Title: "probe", OwnerId: s.Owner()})
		return err
	})

	docID := s.CreateDocument(ctx)
	c := s.dial(ctx, docID, collabclient.Options{URLs: []string{s.HubURL(0)}, Client: 1})
	if err := c.InsertAt(0, 'a'); err != nil {
		t.Fatal(err)
	}
	if err := c.DeleteAt(0); err != nil {
		t.Fatal(err)
	}
	if err := c.WaitAcked(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Versions.Client.Checkpoint(ctx, &versionv1.CheckpointRequest{
		DocId: docID, Name: "gc", RequestId: t.Name() + "-cp",
	}); err != nil {
		t.Fatal(err)
	}
	want := ""
	c.Read(func(r *crdt.Replica) { want, _ = r.Text() })

	deadline := time.Now().Add(15 * time.Second)
	var live bool
	for time.Now().Before(deadline) {
		resp, err := s.Docs.ReadOperations(ctx, &documentv1.ReadOperationsRequest{Id: docID, AfterSeq: 0, Limit: 1})
		if err == nil && resp.GetLiveSeq() > 0 {
			live = true
			break
		}
		time.Sleep(250 * time.Millisecond)
	}
	if !live {
		t.Fatal("live snapshot never appeared")
	}
	c2 := s.dial(ctx, docID, collabclient.Options{URLs: []string{s.HubURL(0)}, Client: 2})
	got := ""
	c2.Read(func(r *crdt.Replica) { got, _ = r.Text() })
	if got != want {
		t.Fatalf("text after gc %q != %q", got, want)
	}
}

func startCommentOnAddr(s *Stack, vs *VersionService, addr string) {
	s.t.Helper()
	dbURL := freshDatabase(s.t)
	s.moreDBs = append(s.moreDBs, dbURL)
	env := append([]string{
		"DATABASE_URL=" + dbURL,
		"GRPC_ADDR=" + addr,
		"DOCUMENT_ADDR=" + s.grpc,
		"VERSION_ADDR=" + vs.addr,
	}, s.authEnv()...)
	start(s.t, "comment-service", commentBinary(s.t), env...)
}
