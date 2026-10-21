package main

import (
	"context"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	documentv1 "github.com/rowjay007/collab-sphere/gen/document/v1"
	versionv1 "github.com/rowjay007/collab-sphere/gen/version/v1"
	"github.com/rowjay007/collab-sphere/services/version-service/internal/docs"
	versiongrpc "github.com/rowjay007/collab-sphere/services/version-service/internal/grpc"
	"github.com/rowjay007/collab-sphere/services/version-service/internal/history"
	"github.com/rowjay007/collab-sphere/services/version-service/internal/store"
)

func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)))

	dbURL := os.Getenv("DATABASE_URL")
	docAddr := os.Getenv("DOCUMENT_ADDR")
	if dbURL == "" || docAddr == "" {
		slog.Error("DATABASE_URL and DOCUMENT_ADDR are required")
		os.Exit(1)
	}
	addr := os.Getenv("GRPC_ADDR")
	if addr == "" {
		addr = ":9092"
	}
	sweep := 5 * time.Second
	if raw := os.Getenv("SWEEP_INTERVAL"); raw != "" {
		d, err := time.ParseDuration(raw)
		if err != nil {
			slog.Error("SWEEP_INTERVAL", "err", err)
			os.Exit(1)
		}
		sweep = d
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
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

	conn, err := grpc.NewClient(docAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		slog.Error("dial document-service", "err", err)
		os.Exit(1)
	}
	defer conn.Close()
	h := history.New(store.New(pool), docs.New(documentv1.NewDocumentServiceClient(conn)))

	lis, err := net.Listen("tcp", addr)
	if err != nil {
		slog.Error("listen", "err", err, "addr", addr)
		os.Exit(1)
	}
	server := grpc.NewServer()
	versionv1.RegisterVersionServiceServer(server, versiongrpc.New(h))

	swept := make(chan struct{})
	if sweep > 0 {
		go func() {
			defer close(swept)
			h.Sweep(ctx, sweep)
		}()
	} else {
		close(swept)
	}

	errCh := make(chan error, 1)
	go func() {
		slog.Info("serving", "addr", lis.Addr().String(), "sweep", sweep.String())
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
	cancel()
	server.GracefulStop()
	<-swept
	if serveErr != nil {
		os.Exit(1)
	}
}
