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

	documentv1 "github.com/rowjay007/collab-sphere/gen/document/v1"
	"github.com/rowjay007/collab-sphere/pkg/access"
	"github.com/rowjay007/collab-sphere/pkg/auth"
	"github.com/rowjay007/collab-sphere/pkg/metrics"
	docgrpc "github.com/rowjay007/collab-sphere/services/document-service/internal/grpc"
	"github.com/rowjay007/collab-sphere/services/document-service/internal/store"
	docworker "github.com/rowjay007/collab-sphere/services/document-service/internal/worker"
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
	pubB64 := os.Getenv("AUTH_PUBLIC_KEY")
	internal := os.Getenv("INTERNAL_TOKEN")
	permAddr := os.Getenv("PERMISSION_ADDR")
	if pubB64 == "" || internal == "" || permAddr == "" {
		slog.Error("AUTH_PUBLIC_KEY, INTERNAL_TOKEN, and PERMISSION_ADDR are required")
		os.Exit(1)
	}
	pub, err := auth.ParsePublicKey(pubB64)
	if err != nil {
		slog.Error("AUTH_PUBLIC_KEY", "err", err)
		os.Exit(1)
	}
	if _, err := auth.ParseInternal(internal); err != nil {
		slog.Error("INTERNAL_TOKEN", "err", err)
		os.Exit(1)
	}
	perms, err := access.Dial(permAddr, internal)
	if err != nil {
		slog.Error("dial permission-service", "err", err)
		os.Exit(1)
	}
	defer perms.Close()

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
	st := store.New(pool)
	if err := st.BackfillReports(ctx); err != nil {
		slog.Error("backfill reports", "err", err)
		os.Exit(1)
	}

	lis, err := net.Listen("tcp", addr)
	if err != nil {
		slog.Error("listen", "err", err, "addr", addr)
		os.Exit(1)
	}

	prom := metrics.NewRegistry()
	docMetrics := metrics.NewDoc(prom)
	stopMetrics, err := metrics.Serve(metrics.Addr(":9190"), prom)
	if err != nil {
		slog.Error("metrics", "err", err)
		os.Exit(1)
	}
	defer stopMetrics()
	server := grpc.NewServer(
		grpc.ChainUnaryInterceptor(
			metrics.UnaryServer(metrics.NewGRPC(prom)),
			auth.UnaryInterceptor(pub, internal, docgrpc.Methods()),
		),
		grpc.StreamInterceptor(auth.StreamInterceptor(pub, internal, docgrpc.Methods())),
	)
	doc := docgrpc.New(st)
	doc.UseReplicaTTL(lookupDuration("REPLICA_REPORT_TTL", 336*time.Hour))
	doc.UsePermissions(perms)
	doc.UseAppend(docMetrics.Append)
	documentv1.RegisterDocumentServiceServer(server, doc)

	gcCtx, gcCancel := context.WithCancel(context.Background())
	defer gcCancel()
	versionAddr := os.Getenv("VERSION_ADDR")
	commentAddr := os.Getenv("COMMENT_ADDR")
	if versionAddr == "" || commentAddr == "" {
		slog.Info("gc worker disabled", "reason", "VERSION_ADDR or COMMENT_ADDR unset")
	} else if interval := gcInterval(); interval <= 0 {
		slog.Info("gc worker disabled", "reason", "GC_INTERVAL empty or zero")
	} else {
		clients, err := docworker.Dial(versionAddr, commentAddr, internal)
		if err != nil {
			slog.Error("gc clients", "err", err)
			os.Exit(1)
		}
		gc := &docworker.GC{
			Store:   st,
			Floor:   clients,
			Pins:    clients,
			Metrics: docMetrics,
			TTL:     lookupDuration("REPLICA_REPORT_TTL", 336*time.Hour),
		}
		go gc.Run(gcCtx, interval)
	}

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

func gcInterval() time.Duration {
	raw, ok := os.LookupEnv("GC_INTERVAL")
	if !ok {
		return 30 * time.Second
	}
	if raw == "" || raw == "0" || raw == "0s" {
		return 0
	}
	d, err := time.ParseDuration(raw)
	if err != nil {
		return 30 * time.Second
	}
	return d
}

func lookupDuration(name string, def time.Duration) time.Duration {
	raw := os.Getenv(name)
	if raw == "" {
		return def
	}
	d, err := time.ParseDuration(raw)
	if err != nil {
		return def
	}
	return d
}
