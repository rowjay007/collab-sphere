package client

import (
	"testing"
	"time"

	collabv1 "github.com/rowjay007/collab-sphere/gen/collab/v1"
	crdtv1 "github.com/rowjay007/collab-sphere/gen/crdt/v1"
)

func TestSocketOpsThenReport(t *testing.T) {
	srv, store := startHub(t)
	docID := canonicalTestDocumentID("doc")
	c := dial(t, srv, "doc", 1)
	op, err := c.insertAt(0, 'a')
	if err != nil {
		t.Fatal(err)
	}
	if err := c.sendClient(&collabv1.ClientMessage{Payload: &collabv1.ClientMessage_VectorReport{VectorReport: &collabv1.VectorReport{
		Vector: []*crdtv1.VectorEntry{
			{Client: uint64(op.ID.Client), Clock: op.ID.Clock + 10},
			{Client: 8, Clock: 3},
		},
	}}}); err != nil {
		t.Fatal(err)
	}

	deadline := time.Now().Add(2 * time.Second)
	for {
		rep, ok := store.Report(docID, 1)
		if ok && rep.HomeSeq == 1 && rep.Vector[op.ID.Client] == op.ID.Clock && len(rep.Vector) == 1 && rep.Vector.Covers(op.ID) {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("report = %+v ok=%v", rep, ok)
		}
		select {
		case <-t.Context().Done():
			t.Fatal(t.Context().Err())
		case <-time.After(5 * time.Millisecond):
		}
	}
}
