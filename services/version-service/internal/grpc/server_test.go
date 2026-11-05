package versiongrpc_test

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	versionv1 "github.com/rowjay007/collab-sphere/gen/version/v1"
	"github.com/rowjay007/collab-sphere/pkg/auth"
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

func TestRestoreFloorRejectsNonInternal(t *testing.T) {
	pub, priv, err := auth.GenerateKeypair()
	if err != nil {
		t.Fatal(err)
	}
	const secret = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
	lis := bufconn.Listen(1024 * 1024)
	server := grpc.NewServer(grpc.UnaryInterceptor(auth.UnaryInterceptor(pub, secret, versiongrpc.Methods())))
	versionv1.RegisterVersionServiceServer(server, versiongrpc.New(nil))
	go server.Serve(lis)
	t.Cleanup(func() {
		server.Stop()
		_ = lis.Close()
	})
	conn, err := grpc.NewClient("passthrough://bufnet",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return lis.Dial() }),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })
	client := versionv1.NewVersionServiceClient(conn)
	tok, err := auth.Mint(priv, "ada", time.Now(), time.Now().Add(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	ctx := metadata.AppendToOutgoingContext(t.Context(), "authorization", "Bearer "+tok)
	_, err = client.RestoreFloor(ctx, &versionv1.RestoreFloorRequest{DocumentId: uuid.NewString()})
	if status.Code(err) != codes.Unauthenticated {
		t.Fatalf("user token: %v", err)
	}
	internal := metadata.AppendToOutgoingContext(t.Context(), "authorization", "Bearer "+auth.FormatInternal(secret))
	_, err = client.RestoreFloor(internal, &versionv1.RestoreFloorRequest{DocumentId: "not-a-uuid"})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("internal token: %v", err)
	}
}
