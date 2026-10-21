package stack

import (
	"context"
	"strings"
	"testing"
	"time"

	documentv1 "github.com/rowjay007/collab-sphere/gen/document/v1"
	"github.com/rowjay007/collab-sphere/pkg/collabclient"
)

func TestForkLiveDocumentRefusesSourceReplica(t *testing.T) {
	s := New(t)
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()

	docID := s.CreateDocument(ctx)
	src := s.DialAllocated(ctx, docID, 0)
	if err := src.InsertAt(0, 'a'); err != nil {
		t.Fatal(err)
	}
	if err := src.WaitAcked(ctx); err != nil {
		t.Fatal(err)
	}

	head, err := s.Docs.GetDocument(ctx, &documentv1.GetDocumentRequest{Id: docID})
	if err != nil {
		t.Fatal(err)
	}
	fork, err := s.Docs.ForkDocument(ctx, &documentv1.ForkDocumentRequest{
		SourceId:  docID,
		Seq:       head.GetOpSeq(),
		Title:     "fork",
		RequestId: t.Name(),
	})
	if err != nil {
		t.Fatal(err)
	}

	branch := s.DialAllocated(ctx, fork.GetId(), 0)
	if branch.ID() == 0 || branch.ID() == src.ID() {
		t.Fatalf("fork replica %d source %d", branch.ID(), src.ID())
	}
	if err := src.InsertAt(1, 'b'); err != nil {
		t.Fatal(err)
	}
	if err := branch.InsertAt(1, 'c'); err != nil {
		t.Fatal(err)
	}
	Settle(t, ctx, s.Docs, docID, []*collabclient.Client{src})
	Settle(t, ctx, s.Docs, fork.GetId(), []*collabclient.Client{branch})

	srcText, _ := LoadText(t, ctx, s.Docs, docID)
	forkText, _ := LoadText(t, ctx, s.Docs, fork.GetId())
	if srcText != "ab" || forkText != "ac" {
		t.Fatalf("source %q fork %q", srcText, forkText)
	}

	_, err = collabclient.DialWith(ctx, fork.GetId(), collabclient.Options{
		URLs:   s.HubURLs(),
		Client: src.ID(),
	})
	if err == nil || !strings.Contains(err.Error(), "CODE_UNKNOWN_REPLICA") {
		t.Fatalf("source replica on fork: %v", err)
	}
}
