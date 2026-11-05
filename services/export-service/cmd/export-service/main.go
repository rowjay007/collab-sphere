package main

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/grpc"

	documentv1 "github.com/rowjay007/collab-sphere/gen/document/v1"
	exportv1 "github.com/rowjay007/collab-sphere/gen/export/v1"
	versionv1 "github.com/rowjay007/collab-sphere/gen/version/v1"
	"github.com/rowjay007/collab-sphere/pkg/access"
	"github.com/rowjay007/collab-sphere/pkg/auth"
	"github.com/rowjay007/collab-sphere/pkg/metrics"
	"github.com/rowjay007/collab-sphere/services/export-service/internal/export"
	exportgrpc "github.com/rowjay007/collab-sphere/services/export-service/internal/grpc"
)

type config struct {
	dbURL        string
	grpcAddr     string
	authPub      string
	internal     string
	documentAddr string
	versionAddr  string
	permAddr     string
	s3           export.S3Config
	crash        string
	pprofAddr    string
}

func loadConfig() (config, error) {
	cfg := config{
		dbURL:        os.Getenv("DATABASE_URL"),
		grpcAddr:     os.Getenv("GRPC_ADDR"),
		authPub:      os.Getenv("AUTH_PUBLIC_KEY"),
		internal:     os.Getenv("INTERNAL_TOKEN"),
		documentAddr: os.Getenv("DOCUMENT_ADDR"),
		versionAddr:  os.Getenv("VERSION_ADDR"),
		permAddr:     os.Getenv("PERMISSION_ADDR"),
		s3: export.S3Config{
			Endpoint:  os.Getenv("S3_ENDPOINT"),
			Bucket:    os.Getenv("S3_BUCKET"),
			Region:    os.Getenv("S3_REGION"),
			AccessKey: os.Getenv("S3_ACCESS_KEY"),
			SecretKey: os.Getenv("S3_SECRET_KEY"),
		},
		crash:     os.Getenv("EXPORT_CRASH_AFTER"),
		pprofAddr: os.Getenv("EXPORT_PPROF_ADDR"),
	}
	if cfg.dbURL == "" {
		return config{}, errors.New("DATABASE_URL is required")
	}
	if cfg.authPub == "" {
		return config{}, errors.New("AUTH_PUBLIC_KEY is required")
	}
	if cfg.internal == "" {
		return config{}, errors.New("INTERNAL_TOKEN is required")
	}
	if cfg.documentAddr == "" {
		return config{}, errors.New("DOCUMENT_ADDR is required")
	}
	if cfg.versionAddr == "" {
		return config{}, errors.New("VERSION_ADDR is required")
	}
	if cfg.permAddr == "" {
		return config{}, errors.New("PERMISSION_ADDR is required")
	}
	if cfg.s3.Endpoint == "" {
		return config{}, errors.New("S3_ENDPOINT is required")
	}
	if cfg.s3.Bucket == "" {
		return config{}, errors.New("S3_BUCKET is required")
	}
	if cfg.s3.Region == "" {
		return config{}, errors.New("S3_REGION is required")
	}
	if cfg.s3.AccessKey == "" {
		return config{}, errors.New("S3_ACCESS_KEY is required")
	}
	if cfg.s3.SecretKey == "" {
		return config{}, errors.New("S3_SECRET_KEY is required")
	}
	switch cfg.crash {
	case "", "upload", "claimed":
	default:
		return config{}, errors.New("EXPORT_CRASH_AFTER must be upload or claimed")
	}
	if cfg.grpcAddr == "" {
		cfg.grpcAddr = ":9095"
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
	if _, err := auth.ParseInternal(cfg.internal); err != nil {
		slog.Error("INTERNAL_TOKEN", "err", err)
		os.Exit(1)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	pool, err := pgxpool.New(ctx, cfg.dbURL)
	if err != nil {
		slog.Error("connect postgres", "err", err)
		os.Exit(1)
	}
	defer pool.Close()
	if err := export.Migrate(ctx, pool); err != nil {
		slog.Error("migrate", "err", err)
		os.Exit(1)
	}

	bucket, err := export.OpenBucket(ctx, cfg.s3)
	if err != nil {
		slog.Error("s3", "err", err)
		os.Exit(1)
	}
	var ensureErr error
	for attempt := 1; attempt <= 30; attempt++ {
		ensureErr = bucket.Ensure(ctx)
		if ensureErr == nil {
			break
		}
		if attempt == 30 || ctx.Err() != nil {
			break
		}
		time.Sleep(time.Second)
	}
	if ensureErr != nil {
		slog.Error("s3 bucket", "err", ensureErr)
		os.Exit(1)
	}

	perms, err := access.Dial(cfg.permAddr, cfg.internal)
	if err != nil {
		slog.Error("dial permission-service", "err", err)
		os.Exit(1)
	}
	defer perms.Close()

	docConn, err := export.DialDocument(cfg.documentAddr, cfg.internal)
	if err != nil {
		slog.Error("dial document-service", "err", err)
		os.Exit(1)
	}
	defer docConn.Close()

	verConn, err := export.DialVersion(cfg.versionAddr, cfg.internal)
	if err != nil {
		slog.Error("dial version-service", "err", err)
		os.Exit(1)
	}
	defer verConn.Close()

	if cfg.pprofAddr != "" {
		go func() {
			if err := http.ListenAndServe(cfg.pprofAddr, nil); err != nil {
				slog.Error("pprof", "err", err)
			}
		}()
	}

	prom := metrics.NewRegistry()
	exportMetrics := metrics.NewExport(prom)
	stopMetrics, err := metrics.Serve(metrics.Addr(":9195"), prom)
	if err != nil {
		slog.Error("metrics", "err", err)
		os.Exit(1)
	}
	defer stopMetrics()
	svc := export.New(export.NewStore(pool), documentv1.NewDocumentServiceClient(docConn), versionv1.NewVersionServiceClient(verConn), perms, bucket, cfg.crash)
	svc.UseMetrics(exportMetrics.Counts, exportMetrics.Seconds)
	go svc.PollJobCounts(ctx)
	go func() {
		if err := svc.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
			slog.Error("worker", "err", err)
		}
	}()

	lis, err := net.Listen("tcp", cfg.grpcAddr)
	if err != nil {
		slog.Error("listen", "err", err, "addr", cfg.grpcAddr)
		os.Exit(1)
	}
	server := grpc.NewServer(
		grpc.ChainUnaryInterceptor(
			metrics.UnaryServer(metrics.NewGRPC(prom)),
			auth.UnaryInterceptor(pub, cfg.internal, exportgrpc.Methods()),
		),
		grpc.StreamInterceptor(auth.StreamInterceptor(pub, cfg.internal, exportgrpc.Methods())),
	)
	exportv1.RegisterExportServiceServer(server, exportgrpc.New(svc))

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
	cancel()
	server.GracefulStop()
	if serveErr != nil {
		os.Exit(1)
	}
}
