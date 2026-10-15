package testredis

import (
	"os"
	"testing"

	"github.com/redis/go-redis/v9"
)

func Client(t *testing.T) *redis.Client {
	t.Helper()
	url := os.Getenv("COLLAB_TEST_REDIS_URL")
	if url == "" {
		t.Skip("COLLAB_TEST_REDIS_URL not set")
	}
	opt, err := redis.ParseURL(url)
	if err != nil {
		t.Fatalf("parse redis url: %v", err)
	}
	rdb := redis.NewClient(opt)
	t.Cleanup(func() { _ = rdb.Close() })
	if err := rdb.Ping(t.Context()).Err(); err != nil {
		t.Fatalf("ping redis: %v", err)
	}
	return rdb
}
