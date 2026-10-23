package versions

import (
	"context"
	"errors"
	"net"
	"testing"

	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	versionv1 "github.com/rowjay007/collab-sphere/gen/version/v1"
	"github.com/rowjay007/collab-sphere/services/comment-service/internal/comments"
)

type frontierStub struct {
	versionv1.UnimplementedVersionServiceServer
	resp *versionv1.FrontierResponse
	err  error
}

func (s frontierStub) Frontier(context.Context, *versionv1.FrontierRequest) (*versionv1.FrontierResponse, error) {
	return s.resp, s.err
}

func TestFrontierClientMapsUnavailable(t *testing.T) {
	conn, err := grpc.NewClient("127.0.0.1:1", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })
	c := NewFrontierClient(versionv1.NewVersionServiceClient(conn))
	got, err := c.Frontier(t.Context(), uuid.New())
	if !errors.Is(err, comments.ErrUnavailable) {
		t.Fatalf("err = %v", err)
	}
	if got != nil {
		t.Fatalf("fallback frontier %v", got)
	}
}

func TestFrontierClientMapsUnavailableStatus(t *testing.T) {
	c, stop := dialFrontier(t, frontierStub{err: status.Error(codes.Unavailable, "down")})
	t.Cleanup(stop)
	got, err := c.Frontier(t.Context(), uuid.New())
	if !errors.Is(err, comments.ErrUnavailable) {
		t.Fatalf("err = %v", err)
	}
	if got != nil {
		t.Fatalf("fallback frontier %v", got)
	}
}

func TestFrontierClientParsesEntries(t *testing.T) {
	home := uuid.MustParse("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa")
	other := uuid.MustParse("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb")
	c, stop := dialFrontier(t, frontierStub{
		resp: &versionv1.FrontierResponse{Entries: []*versionv1.FrontierEntry{
			{DocumentId: home.String(), Seq: 10},
			{DocumentId: other.String(), Seq: 4},
		}},
	})
	t.Cleanup(stop)
	got, err := c.Frontier(t.Context(), uuid.New())
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || got[home] != 10 || got[other] != 4 {
		t.Fatalf("got %v", got)
	}
}

func dialFrontier(t *testing.T, stub frontierStub) (*FrontierClient, func()) {
	t.Helper()
	lis := bufconn.Listen(1024 * 1024)
	server := grpc.NewServer()
	versionv1.RegisterVersionServiceServer(server, stub)
	go server.Serve(lis)
	conn, err := grpc.NewClient("passthrough://bufnet",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return lis.Dial()
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatal(err)
	}
	return NewFrontierClient(versionv1.NewVersionServiceClient(conn)), func() {
		_ = conn.Close()
		server.Stop()
		_ = lis.Close()
	}
}
