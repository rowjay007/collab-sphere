package main

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/redis/go-redis/v9"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	documentv1 "github.com/rowjay007/collab-sphere/gen/document/v1"
	permissionv1 "github.com/rowjay007/collab-sphere/gen/permission/v1"
	presencev1 "github.com/rowjay007/collab-sphere/gen/presence/v1"
	"github.com/rowjay007/collab-sphere/pkg/access"
	"github.com/rowjay007/collab-sphere/pkg/auth"
	"github.com/rowjay007/collab-sphere/pkg/metrics"
	"github.com/rowjay007/collab-sphere/services/collab-hub/internal/client"
	"github.com/rowjay007/collab-sphere/services/collab-hub/internal/docstore"
	"github.com/rowjay007/collab-sphere/services/collab-hub/internal/fanout"
	"github.com/rowjay007/collab-sphere/services/collab-hub/internal/hub"
	"github.com/rowjay007/collab-sphere/services/collab-hub/internal/presence"
)

const presenceFlush = 16 * time.Millisecond

func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)))

	httpAddr := envOr("HTTP_ADDR", ":8080")
	docAddr := envOr("DOCUMENT_SERVICE_ADDR", "localhost:9090")
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

	conn, err := grpc.NewClient(docAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithUnaryInterceptor(auth.InternalUnary(internal)),
	)
	if err != nil {
		slog.Error("dial document-service", "addr", docAddr, "err", err)
		os.Exit(1)
	}
	defer conn.Close()

	busCtx, stopBus := context.WithCancel(context.Background())
	busDone := make(chan struct{})
	var bus fanout.Bus
	if redisURL := os.Getenv("REDIS_URL"); redisURL != "" {
		opts, err := redis.ParseURL(redisURL)
		if err != nil {
			slog.Error("parse REDIS_URL", "err", err)
			os.Exit(1)
		}
		rdb := redis.NewClient(opts)
		defer rdb.Close()
		r := fanout.NewRedis(rdb, hubID())
		go func() {
			defer close(busDone)
			_ = r.Run(busCtx)
		}()
		bus = r
	} else {
		close(busDone)
		bus = fanout.NewMemory()
	}

	var pres hub.Presence
	var writer *presence.Writer
	if presenceAddr := os.Getenv("PRESENCE_SERVICE_ADDR"); presenceAddr != "" {
		heartbeat, err := time.ParseDuration(envOr("PRESENCE_HEARTBEAT", "10s"))
		if err != nil || heartbeat <= 0 {
			slog.Error("parse PRESENCE_HEARTBEAT", "err", err)
			os.Exit(1)
		}
		pconn, err := grpc.NewClient(presenceAddr,
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithUnaryInterceptor(auth.InternalUnary(internal)),
		)
		if err != nil {
			slog.Error("dial presence-service", "addr", presenceAddr, "err", err)
			os.Exit(1)
		}
		defer pconn.Close()
		writer = presence.NewWriter(presencev1.NewPresenceServiceClient(pconn), presenceFlush, heartbeat)
		pres = writer
	} else {
		pres = presence.NewLocal(bus.Loopback)
	}

	reg := hub.NewRegistry(docstore.New(documentv1.NewDocumentServiceClient(conn)), bus, pres)
	if v := os.Getenv("HUB_IDLE_SYNC"); v != "" {
		idle, err := time.ParseDuration(v)
		if err != nil || idle <= 0 {
			slog.Error("parse HUB_IDLE_SYNC", "err", err)
			os.Exit(1)
		}
		reg.SetIdleSync(idle)
	}
	prom := metrics.NewRegistry()
	hubMetrics := metrics.NewHub(prom)
	reg.SetMetrics(hubMetrics)
	stopMetrics, err := metrics.Serve(metrics.Addr(":9196"), prom)
	if err != nil {
		slog.Error("metrics", "err", err)
		os.Exit(1)
	}
	defer stopMetrics()
	reg.SetSecurity(pub, hub.ObservePolicy(permPolicy{c: perms}, hubMetrics.PolicyDump))
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	mux.Handle("GET /ws/{docID}", client.Handler(reg))

	srv := &http.Server{Addr: httpAddr, Handler: mux}
	go func() {
		slog.Info("listen", "addr", httpAddr)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			slog.Error("listen", "err", err)
			os.Exit(1)
		}
	}()

	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	<-sigs

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	err = srv.Shutdown(ctx)
	reg.Close()
	if writer != nil {
		writer.Close()
	}
	stopBus()
	<-busDone
	if err != nil {
		slog.Error("shutdown", "err", err)
		os.Exit(1)
	}
}

type permPolicy struct{ c *access.Client }

func (p permPolicy) Dump(ctx context.Context, docID string) (map[string]permissionv1.Role, uint32, string, error) {
	resp, err := p.c.Dump(ctx, docID)
	if err != nil {
		return nil, 0, "", err
	}
	return resp.GetMembers(), resp.GetWriteEpoch(), resp.GetEpochState(), nil
}

func hubID() string {
	if id := os.Getenv("HUB_ID"); id != "" {
		return id
	}
	host, err := os.Hostname()
	if err != nil {
		host = "collab-hub"
	}
	return fmt.Sprintf("%s-%d", host, os.Getpid())
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
