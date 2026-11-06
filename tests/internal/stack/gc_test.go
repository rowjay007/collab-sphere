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
	ctx, cancel := context.WithTimeout(t.Context(), 90*time.Second)
	defer cancel()
	restartDocumentGC(t, s, 30*time.Minute)

	docID := s.CreateDocument(ctx)
	meta, err := s.Docs.GetDocument(ctx, &documentv1.GetDocumentRequest{Id: docID})
	if err != nil {
		t.Fatal(err)
	}
	c := s.dial(ctx, docID, collabclient.Options{URLs: []string{s.HubURL(0)}, Client: 1})
	var gone, kept crdt.ID
	if err := c.Edit(func(r *crdt.Replica) ([]crdt.Op, error) {
		a, err := r.InsertAfter(crdt.Root, 'a')
		if err != nil {
			return nil, err
		}
		b, err := r.InsertAfter(crdt.Root, 'b')
		if err != nil {
			return nil, err
		}
		del, err := r.Delete(a.ID)
		if err != nil {
			return nil, err
		}
		gone, kept = a.ID, b.ID
		return []crdt.Op{a, b, del}, nil
	}); err != nil {
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
	ageAllButTipVersion(t, s.Versions.DB(), docID)
	c.Disconnect()
	ageAllReplicaReports(t, s.dbURL, meta.GetForkRoot(), 31*time.Minute)

	waitLiveState(t, ctx, s, docID, func(*crdt.Doc) bool {
		return !liveDoc(t, s.dbURL, docID).Has(gone)
	})
	s.StopHub(0)
	s.StartHub(0)

	c2 := s.dial(ctx, docID, collabclient.Options{URLs: []string{s.HubURL(0)}, Client: 2})
	joined, err := crdt.FromSnapshot(c2.Snapshot())
	if err != nil {
		t.Fatal(err)
	}
	if joined.Has(gone) {
		t.Fatalf("cold hub served collected node %v", gone)
	}
	if !joined.Has(kept) {
		t.Fatalf("cold hub lost live node %v", kept)
	}
	if got := joined.Collected()[1]; got < 3 {
		t.Fatalf("cold hub collected[1] = %d, want >= 3", got)
	}
	if text := c2.Text(); text != "b" {
		t.Fatalf("text after cold join = %q, want %q", text, "b")
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
