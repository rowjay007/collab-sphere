package stack

import (
	"context"
	"testing"

	"google.golang.org/protobuf/proto"

	documentv1 "github.com/rowjay007/collab-sphere/gen/document/v1"
	"github.com/rowjay007/collab-sphere/pkg/collabclient"
	"github.com/rowjay007/collab-sphere/pkg/crdt"
)

// Settle waits until every client's ops are echoed and every client has
// received every other client's ops, then asserts identical state.
func Settle(t *testing.T, ctx context.Context, clients []*collabclient.Client) string {
	t.Helper()
	for _, c := range clients {
		if err := c.WaitAcked(ctx); err != nil {
			t.Fatalf("wait acked: %v (errors from hub: %v)", err, c.Errors())
		}
	}
	var last []crdt.ID
	for _, c := range clients {
		c.Read(func(r *crdt.Replica) {
			if clock := r.Vector()[r.Client()]; clock > 0 {
				last = append(last, crdt.ID{Clock: clock, Client: r.Client()})
			}
		})
	}
	for _, c := range clients {
		err := c.WaitUntil(ctx, func(r *crdt.Replica) bool {
			v := r.Vector()
			for _, id := range last {
				if !v.Covers(id) {
					return false
				}
			}
			return true
		})
		if err != nil {
			t.Fatalf("wait for peers' ops: %v", err)
		}
	}
	want := clients[0].Snapshot()
	text := clients[0].Text()
	for i, c := range clients {
		if errs := c.Errors(); len(errs) > 0 {
			t.Fatalf("client %d got hub errors: %v", i, errs)
		}
		if got := c.Text(); got != text {
			t.Fatalf("client %d text diverged:\n got %q\nwant %q", i, got, text)
		}
		if !proto.Equal(c.Snapshot(), want) {
			t.Fatalf("client %d snapshot diverged from client 0", i)
		}
	}
	return text
}

func LoadText(t *testing.T, ctx context.Context, docs documentv1.DocumentServiceClient, docID string) (string, *documentv1.Document) {
	t.Helper()
	resp, err := docs.LoadState(ctx, &documentv1.LoadStateRequest{Id: docID})
	if err != nil {
		t.Fatal(err)
	}
	doc, err := crdt.FromSnapshot(resp.GetSnapshot())
	if err != nil {
		t.Fatal(err)
	}
	for _, p := range resp.GetOperations() {
		op, err := crdt.OpFromProto(p)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := doc.Apply(op); err != nil {
			t.Fatal(err)
		}
	}
	return doc.Text(), resp.GetDocument()
}
