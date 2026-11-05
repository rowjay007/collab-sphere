package main

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"syscall"

	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/grpc"

	permissionv1 "github.com/rowjay007/collab-sphere/gen/permission/v1"
	"github.com/rowjay007/collab-sphere/pkg/auth"
	permgrpc "github.com/rowjay007/collab-sphere/services/permission-service/internal/grpc"
	"github.com/rowjay007/collab-sphere/services/permission-service/internal/perms"
)

type config struct {
	dbURL        string
	grpcAddr     string
	authPub      string
	sharePriv    string
	internal     string
	documentAddr string
}

func loadConfig() (config, error) {
	cfg := config{
		dbURL:        os.Getenv("DATABASE_URL"),
		grpcAddr:     os.Getenv("GRPC_ADDR"),
		authPub:      os.Getenv("AUTH_PUBLIC_KEY"),
		sharePriv:    os.Getenv("SHARE_PRIVATE_KEY"),
		internal:     os.Getenv("INTERNAL_TOKEN"),
		documentAddr: os.Getenv("DOCUMENT_ADDR"),
	}
	if cfg.dbURL == "" {
		return config{}, errors.New("DATABASE_URL is required")
	}
	if cfg.authPub == "" {
		return config{}, errors.New("AUTH_PUBLIC_KEY is required")
	}
	if cfg.sharePriv == "" {
		return config{}, errors.New("SHARE_PRIVATE_KEY is required")
	}
	if cfg.internal == "" {
		return config{}, errors.New("INTERNAL_TOKEN is required")
	}
	if cfg.documentAddr == "" {
		return config{}, errors.New("DOCUMENT_ADDR is required")
	}
	if cfg.grpcAddr == "" {
		cfg.grpcAddr = ":9094"
	}
	return cfg, nil
}

func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)))

	cfg, err := loadConfig()
	if err != nil {
		slog.Error(err.Error())
		os.Exit(1)
	}
	pub, err := auth.ParsePublicKey(cfg.authPub)
	if err != nil {
		slog.Error("AUTH_PUBLIC_KEY", "err", err)
		os.Exit(1)
	}
	share, err := auth.ParsePrivateKey(cfg.sharePriv)
	if err != nil {
		slog.Error("SHARE_PRIVATE_KEY", "err", err)
		os.Exit(1)
	}
	if _, err := auth.ParseInternal(cfg.internal); err != nil {
		slog.Error("INTERNAL_TOKEN", "err", err)
		os.Exit(1)
	}

	ctx := context.Background()
	pool, err := pgxpool.New(ctx, cfg.dbURL)
	if err != nil {
		slog.Error("connect postgres", "err", err)
		os.Exit(1)
	}
	defer pool.Close()

	if err := perms.Migrate(ctx, pool); err != nil {
		slog.Error("migrate", "err", err)
		os.Exit(1)
	}

	lis, err := net.Listen("tcp", cfg.grpcAddr)
	if err != nil {
		slog.Error("listen", "err", err, "addr", cfg.grpcAddr)
		os.Exit(1)
	}

	svc := perms.New(pool, share)
	server := grpc.NewServer(
		grpc.UnaryInterceptor(auth.UnaryInterceptor(pub, cfg.internal, permgrpc.Methods())),
		grpc.StreamInterceptor(auth.StreamInterceptor(pub, cfg.internal, permgrpc.Methods())),
	)
	permissionv1.RegisterPermissionServiceServer(server, permgrpc.New(svc))

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
	if serveErr != nil {
		os.Exit(1)
	}
}
