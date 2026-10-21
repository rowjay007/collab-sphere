package stack

import (
	"context"
	"net"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	commentv1 "github.com/rowjay007/collab-sphere/gen/comment/v1"
	"github.com/rowjay007/collab-sphere/services/comment-service/inproc"
)

type CommentVersions interface {
	Frontier(ctx context.Context, doc uuid.UUID) (map[uuid.UUID]uint64, error)
}

func StartCommentService(s *Stack, versions CommentVersions) commentv1.CommentServiceClient {
	s.t.Helper()
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, freshDatabase(s.t))
	if err != nil {
		s.t.Fatal(err)
	}
	s.t.Cleanup(pool.Close)
	if err := inproc.Migrate(ctx, pool); err != nil {
		s.t.Fatal(err)
	}

	addr := freeAddr(s.t)
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		s.t.Fatal(err)
	}
	server := inproc.NewServer(pool, s.Docs, versions)
	go server.Serve(lis)
	s.t.Cleanup(func() {
		server.GracefulStop()
		_ = lis.Close()
	})

	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		s.t.Fatal(err)
	}
	s.t.Cleanup(func() { conn.Close() })
	client := commentv1.NewCommentServiceClient(conn)
	waitFor(s.t, "comment-service", func(ctx context.Context) error {
		_, err := client.ListThreads(ctx, &commentv1.ListThreadsRequest{DocumentId: uuid.NewString()})
		if status.Code(err) == codes.NotFound || status.Code(err) == codes.InvalidArgument || err == nil {
			return nil
		}
		return err
	})
	return client
}
