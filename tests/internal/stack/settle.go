package stack

import (
	"context"
	"testing"

	"google.golang.org/protobuf/proto"

	collabv1 "github.com/rowjay007/collab-sphere/gen/collab/v1"
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
		for _, err := range c.Errors() {
			if err.GetCode() != collabv1.ErrorMessage_CODE_UNAVAILABLE {
				t.Fatalf("client %d got hub error: %v", i, err)
			}
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
	doc, meta := loadState(t, ctx, docs, docID)
	return doc.Text(), meta
}

// AssertExactlyOnce checks that the log holds each generated op exactly once:
// op_seq equals the number of distinct generated op IDs, and the stored
// document contains every one of them.
func AssertExactlyOnce(t *testing.T, ctx context.Context, docs documentv1.DocumentServiceClient, docID string, generated []crdt.ID) {
	t.Helper()
	doc, meta := loadState(t, ctx, docs, docID)
	distinct := make(map[crdt.ID]struct{}, len(generated))
	for _, id := range generated {
		distinct[id] = struct{}{}
	}
	if meta.GetOpSeq() != uint64(len(distinct)) {
		t.Fatalf("op_seq = %d, want %d distinct generated ops", meta.GetOpSeq(), len(distinct))
	}
	for id := range distinct {
		if !doc.Contains(id) {
			t.Fatalf("stored document lacks generated op %s", id)
		}
	}
}

func loadState(t *testing.T, ctx context.Context, docs documentv1.DocumentServiceClient, docID string) (*crdt.Doc, *documentv1.Document) {
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
	return doc, resp.GetDocument()
}
