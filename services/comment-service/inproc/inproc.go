package inproc

import (
	"context"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/grpc"

	commentv1 "github.com/rowjay007/collab-sphere/gen/comment/v1"
	documentv1 "github.com/rowjay007/collab-sphere/gen/document/v1"
	"github.com/rowjay007/collab-sphere/services/comment-service/internal/comments"
	"github.com/rowjay007/collab-sphere/services/comment-service/internal/docs"
	commentgrpc "github.com/rowjay007/collab-sphere/services/comment-service/internal/grpc"
)

type Versions interface {
	Frontier(ctx context.Context, doc uuid.UUID) (map[uuid.UUID]uint64, error)
}

func Migrate(ctx context.Context, pool *pgxpool.Pool) error {
	return comments.Migrate(ctx, pool)
}

func NewServer(pool *pgxpool.Pool, docsClient documentv1.DocumentServiceClient, versions Versions) *grpc.Server {
	svc := comments.New(pool, docs.New(docsClient), versions)
	server := grpc.NewServer()
	commentv1.RegisterCommentServiceServer(server, commentgrpc.New(svc))
	return server
}
