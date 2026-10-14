package docstore

import (
	"context"
	"strings"
	"testing"

	documentv1 "github.com/rowjay007/collab-sphere/gen/document/v1"
	"google.golang.org/grpc"
)

type archivedClient struct {
	documentv1.DocumentServiceClient
}

func (archivedClient) LoadState(context.Context, *documentv1.LoadStateRequest, ...grpc.CallOption) (*documentv1.LoadStateResponse, error) {
	return &documentv1.LoadStateResponse{
		Document: &documentv1.Document{Status: documentv1.Document_STATUS_ARCHIVED},
	}, nil
}

func TestLoadRejectsArchivedDocument(t *testing.T) {
	_, err := New(archivedClient{}).Load(t.Context(), "doc")
	if err == nil || !strings.Contains(err.Error(), "document archived") {
		t.Fatalf("Load error = %v", err)
	}
}
