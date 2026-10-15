package main

import (
	"context"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/redis/go-redis/v9"
	"google.golang.org/grpc"

	presencev1 "github.com/rowjay007/collab-sphere/gen/presence/v1"
	presgrpc "github.com/rowjay007/collab-sphere/services/presence-service/internal/grpc"
	"github.com/rowjay007/collab-sphere/services/presence-service/internal/store"
)

func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)))

	redisURL := os.Getenv("REDIS_URL")
	if redisURL == "" {
		slog.Error("REDIS_URL is required")
		os.Exit(1)
	}
	addr := os.Getenv("GRPC_ADDR")
	if addr == "" {
		addr = ":9091"
	}
	ttl, err := envDuration("PRESENCE_TTL", "30s")
	if err != nil {
		slog.Error("PRESENCE_TTL", "err", err)
		os.Exit(1)
	}
	sweepEvery, err := envDuration("PRESENCE_SWEEP", "1s")
	if err != nil {
		slog.Error("PRESENCE_SWEEP", "err", err)
		os.Exit(1)
	}

	opt, err := redis.ParseURL(redisURL)
	if err != nil {
		slog.Error("REDIS_URL", "err", err)
		os.Exit(1)
	}
	ctx := context.Background()
	rdb := redis.NewClient(opt)
	defer rdb.Close()
	if err := rdb.Ping(ctx).Err(); err != nil {
		slog.Error("connect redis", "err", err)
		os.Exit(1)
	}

	lis, err := net.Listen("tcp", addr)
	if err != nil {
		slog.Error("listen", "err", err, "addr", addr)
		os.Exit(1)
	}

	st := store.New(rdb, ttl)
	server := grpc.NewServer()
	presencev1.RegisterPresenceServiceServer(server, presgrpc.New(st))

	sweepCtx, stopSweep := context.WithCancel(context.Background())
	go runSweep(sweepCtx, st, sweepEvery)

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
	stopSweep()
	server.GracefulStop()
	if serveErr != nil {
		os.Exit(1)
	}
}

func runSweep(ctx context.Context, st *store.Store, every time.Duration) {
	ticker := time.NewTicker(every)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := drainSweep(ctx, st, 256); err != nil && ctx.Err() == nil {
				slog.Error("sweep", "err", err)
			}
		}
	}
}

func drainSweep(ctx context.Context, st *store.Store, limit int) error {
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		n, err := st.Sweep(ctx, limit)
		if err != nil {
			return err
		}
		if n < limit {
			return nil
		}
	}
}

func envDuration(key, fallback string) (time.Duration, error) {
	v := os.Getenv(key)
	if v == "" {
		v = fallback
	}
	return time.ParseDuration(v)
}
