package versiongrpc_test

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	versionv1 "github.com/rowjay007/collab-sphere/gen/version/v1"
	versiongrpc "github.com/rowjay007/collab-sphere/services/version-service/internal/grpc"
)

func TestUserMethodRejectsBareContext(t *testing.T) {
	s := versiongrpc.New(nil)
	_, err := s.Checkpoint(context.Background(), &versionv1.CheckpointRequest{
		DocId: uuid.NewString(),
		Name:  "x",
	})
	if status.Code(err) != codes.Unauthenticated {
		t.Fatalf("bare checkpoint: %v", err)
	}
}
