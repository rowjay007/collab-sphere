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
	"google.golang.org/grpc/credentials/insecure"

	commentv1 "github.com/rowjay007/collab-sphere/gen/comment/v1"
	documentv1 "github.com/rowjay007/collab-sphere/gen/document/v1"
	versionv1 "github.com/rowjay007/collab-sphere/gen/version/v1"
	"github.com/rowjay007/collab-sphere/pkg/access"
	"github.com/rowjay007/collab-sphere/pkg/auth"
	"github.com/rowjay007/collab-sphere/pkg/metrics"
	"github.com/rowjay007/collab-sphere/services/comment-service/internal/comments"
	"github.com/rowjay007/collab-sphere/services/comment-service/internal/docs"
	commentgrpc "github.com/rowjay007/collab-sphere/services/comment-service/internal/grpc"
	"github.com/rowjay007/collab-sphere/services/comment-service/internal/versions"
)

type config struct {
	dbURL        string
	grpcAddr     string
	documentAddr string
	versionAddr  string
	authPub      string
	internal     string
	permAddr     string
}

func loadConfig() (config, error) {
	cfg := config{
		dbURL:        os.Getenv("DATABASE_URL"),
		grpcAddr:     os.Getenv("GRPC_ADDR"),
		documentAddr: os.Getenv("DOCUMENT_ADDR"),
		versionAddr:  os.Getenv("VERSION_ADDR"),
		authPub:      os.Getenv("AUTH_PUBLIC_KEY"),
		internal:     os.Getenv("INTERNAL_TOKEN"),
		permAddr:     os.Getenv("PERMISSION_ADDR"),
	}
	if cfg.dbURL == "" {
		return config{}, errors.New("DATABASE_URL is required")
	}
	if cfg.documentAddr == "" {
		return config{}, errors.New("DOCUMENT_ADDR is required")
	}
	if cfg.versionAddr == "" {
		return config{}, errors.New("VERSION_ADDR is required")
	}
	if cfg.authPub == "" {
		return config{}, errors.New("AUTH_PUBLIC_KEY is required")
	}
	if cfg.internal == "" {
		return config{}, errors.New("INTERNAL_TOKEN is required")
	}
	if cfg.permAddr == "" {
		return config{}, errors.New("PERMISSION_ADDR is required")
	}
	if cfg.grpcAddr == "" {
		cfg.grpcAddr = ":9092"
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

	ctx := context.Background()
	pool, err := pgxpool.New(ctx, cfg.dbURL)
	if err != nil {
		slog.Error("connect postgres", "err", err)
		os.Exit(1)
	}
	defer pool.Close()

	if err := comments.Migrate(ctx, pool); err != nil {
		slog.Error("migrate", "err", err)
		os.Exit(1)
	}

	pub, err := auth.ParsePublicKey(cfg.authPub)
	if err != nil {
		slog.Error("AUTH_PUBLIC_KEY", "err", err)
		os.Exit(1)
	}
	if _, err := auth.ParseInternal(cfg.internal); err != nil {
		slog.Error("INTERNAL_TOKEN", "err", err)
		os.Exit(1)
	}
	perms, err := access.Dial(cfg.permAddr, cfg.internal)
	if err != nil {
		slog.Error("dial permission-service", "err", err)
		os.Exit(1)
	}
	defer perms.Close()

	docConn, err := grpc.NewClient(cfg.documentAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithUnaryInterceptor(auth.InternalUnary(cfg.internal)),
	)
	if err != nil {
		slog.Error("dial document-service", "err", err)
		os.Exit(1)
	}
	defer docConn.Close()

	verConn, err := grpc.NewClient(cfg.versionAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithUnaryInterceptor(auth.InternalUnary(cfg.internal)),
	)
	if err != nil {
		slog.Error("dial version-service", "err", err)
		os.Exit(1)
	}
	defer verConn.Close()

	lis, err := net.Listen("tcp", cfg.grpcAddr)
	if err != nil {
		slog.Error("listen", "err", err, "addr", cfg.grpcAddr)
		os.Exit(1)
	}

	svc := comments.New(pool, docs.New(documentv1.NewDocumentServiceClient(docConn)), versions.NewFrontierClient(versionv1.NewVersionServiceClient(verConn)))
	api := commentgrpc.New(svc)
	api.UsePermissions(perms)
	prom := metrics.NewRegistry()
	stopMetrics, err := metrics.Serve(metrics.Addr(":9192"), prom)
	if err != nil {
		slog.Error("metrics", "err", err)
		os.Exit(1)
	}
	defer stopMetrics()
	server := grpc.NewServer(
		grpc.ChainUnaryInterceptor(
			metrics.UnaryServer(metrics.NewGRPC(prom)),
			auth.UnaryInterceptor(pub, cfg.internal, commentgrpc.Methods()),
		),
		grpc.StreamInterceptor(auth.StreamInterceptor(pub, cfg.internal, commentgrpc.Methods())),
	)
	commentv1.RegisterCommentServiceServer(server, api)

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
