package main

import (
	"context"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"syscall"

	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/grpc"

	documentv1 "github.com/rowjay007/collab-sphere/gen/document/v1"
	docgrpc "github.com/rowjay007/collab-sphere/services/document-service/internal/grpc"
	"github.com/rowjay007/collab-sphere/services/document-service/internal/store"
)

func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)))

	dbURL := os.Getenv("DATABASE_URL")
	if dbURL == "" {
		slog.Error("DATABASE_URL is required")
		os.Exit(1)
	}
	addr := os.Getenv("GRPC_ADDR")
	if addr == "" {
		addr = ":9090"
	}

	ctx := context.Background()
	pool, err := pgxpool.New(ctx, dbURL)
	if err != nil {
		slog.Error("connect postgres", "err", err)
		os.Exit(1)
	}
	defer pool.Close()

	if err := store.Migrate(ctx, pool); err != nil {
		slog.Error("migrate", "err", err)
		os.Exit(1)
	}

	lis, err := net.Listen("tcp", addr)
	if err != nil {
		slog.Error("listen", "err", err, "addr", addr)
		os.Exit(1)
	}

	server := grpc.NewServer()
	st := store.New(pool)
	documentv1.RegisterDocumentServiceServer(server, docgrpc.New(st))

	errCh := make(chan error, 1)
	go func() {
		slog.Info("serving", "addr", lis.Addr().String())
		errCh <- server.Serve(lis)
	}()

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)
	var serveErr error
	select {
	case serveErr = <-errCh:
		if serveErr != nil {
			slog.Error("serve", "err", serveErr)
		}
	case <-stop:
	}
	server.GracefulStop()
	st.Close()
	if serveErr != nil {
		pool.Close()
		os.Exit(1)
	}
}
