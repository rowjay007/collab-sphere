package testdb

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/hex"
	"os"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/rowjay007/collab-sphere/pkg/auth"
	"github.com/rowjay007/collab-sphere/services/permission-service/internal/perms"
)

func Open(t *testing.T) (*pgxpool.Pool, ed25519.PrivateKey) {
	t.Helper()
	dsn := os.Getenv("COLLAB_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("COLLAB_TEST_DATABASE_URL not set")
	}

	ctx := context.Background()
	schema := "ptest_" + randomHex(t, 8)

	admin, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatalf("connect admin: %v", err)
	}
	if _, err := admin.Exec(ctx, "CREATE SCHEMA "+schema); err != nil {
		admin.Close(ctx)
		t.Fatalf("create schema: %v", err)
	}
	t.Cleanup(func() {
		_, _ = admin.Exec(context.Background(), "DROP SCHEMA "+schema+" CASCADE")
		_ = admin.Close(context.Background())
	})

	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		t.Fatalf("parse config: %v", err)
	}
	cfg.ConnConfig.RuntimeParams["search_path"] = schema
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatalf("open pool: %v", err)
	}
	t.Cleanup(pool.Close)

	if err := perms.Migrate(ctx, pool); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	_, share, err := auth.GenerateKeypair()
	if err != nil {
		t.Fatal(err)
	}
	return pool, share
}

func randomHex(t *testing.T, n int) string {
	t.Helper()
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		t.Fatalf("rand: %v", err)
	}
	return hex.EncodeToString(b)
}
