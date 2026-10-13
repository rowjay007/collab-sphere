package main

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	documentv1 "github.com/rowjay007/collab-sphere/gen/document/v1"
	"github.com/rowjay007/collab-sphere/services/collab-hub/internal/client"
	"github.com/rowjay007/collab-sphere/services/collab-hub/internal/docstore"
	"github.com/rowjay007/collab-sphere/services/collab-hub/internal/hub"
)

func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)))

	httpAddr := envOr("HTTP_ADDR", ":8080")
	docAddr := envOr("DOCUMENT_SERVICE_ADDR", "localhost:9090")

	conn, err := grpc.NewClient(docAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		slog.Error("dial document-service", "addr", docAddr, "err", err)
		os.Exit(1)
	}
	defer conn.Close()

	reg := hub.NewRegistry(docstore.New(documentv1.NewDocumentServiceClient(conn)))
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

	reg.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := srv.Shutdown(ctx); err != nil {
		slog.Error("shutdown", "err", err)
		os.Exit(1)
	}
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
