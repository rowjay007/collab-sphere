package stack

import (
	"context"
	"testing"
	"time"

	crdtv1 "github.com/rowjay007/collab-sphere/gen/crdt/v1"
	documentv1 "github.com/rowjay007/collab-sphere/gen/document/v1"
	"github.com/rowjay007/collab-sphere/pkg/collabclient"
	"github.com/rowjay007/collab-sphere/pkg/crdt"
)

func TestReplicaReportsCoverAckedOps(t *testing.T) {
	s := New(t)
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	docID := s.CreateDocument(ctx)
	doc, err := s.Docs.GetDocument(ctx, &documentv1.GetDocumentRequest{Id: docID})
	if err != nil {
		t.Fatal(err)
	}
	opts := func(client crdt.ClientID) collabclient.Options {
		return collabclient.Options{URLs: []string{s.HubURL(0)}, Client: client, ReportInterval: 50 * time.Millisecond}
	}
	a := s.dial(ctx, docID, opts(1))
	b := s.dial(ctx, docID, opts(2))
	if err := a.InsertAt(0, 'a'); err != nil {
		t.Fatal(err)
	}
	if err := b.InsertAt(0, 'b'); err != nil {
		t.Fatal(err)
	}
	if err := a.WaitAcked(ctx); err != nil {
		t.Fatal(err)
	}
	if err := b.WaitAcked(ctx); err != nil {
		t.Fatal(err)
	}
	want := map[uint64][]crdt.ID{
		uint64(a.ID()): ownIDs(t, a),
		uint64(b.ID()): ownIDs(t, b),
	}

	deadline := time.Now().Add(10 * time.Second)
	var last *documentv1.ListReportsResponse
	for {
		last, err = s.Docs.ListReports(ctx, &documentv1.ListReportsRequest{ForkRoot: doc.GetForkRoot()})
		if err != nil {
			t.Fatal(err)
		}
		if reportsCover(last.GetReports(), want) {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("reports = %v", last.GetReports())
		}
		select {
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		case <-time.After(20 * time.Millisecond):
		}
	}
}

func ownIDs(t *testing.T, c *collabclient.Client) []crdt.ID {
	t.Helper()
	id := c.ID()
	var ids []crdt.ID
	c.Read(func(r *crdt.Replica) {
		if r == nil {
			return
		}
		for _, op := range r.Ops() {
			if op.OpID().Client == id {
				ids = append(ids, op.OpID())
			}
		}
	})
	if len(ids) == 0 {
		t.Fatalf("client %d has no ops", c.ID())
	}
	return ids
}

func reportsCover(reports []*documentv1.ReplicaReport, want map[uint64][]crdt.ID) bool {
	for replica, ids := range want {
		var vector []*crdtv1.VectorEntry
		found := false
		for _, report := range reports {
			if report.GetReplicaId() == replica {
				vector = report.GetVector()
				found = true
				break
			}
		}
		if !found {
			return false
		}
		for _, id := range ids {
			if !entryCovers(vector, id) {
				return false
			}
		}
	}
	return true
}

func entryCovers(entries []*crdtv1.VectorEntry, id crdt.ID) bool {
	for _, entry := range entries {
		if entry.GetClient() == uint64(id.Client) && entry.GetClock() >= id.Clock {
			return true
		}
	}
	return false
}
