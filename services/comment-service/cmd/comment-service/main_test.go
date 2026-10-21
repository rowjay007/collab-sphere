package main

import "testing"

func TestLoadConfigRequiresVersionAddr(t *testing.T) {
	t.Setenv("DATABASE_URL", "postgres://x")
	t.Setenv("DOCUMENT_ADDR", "127.0.0.1:9090")
	t.Setenv("VERSION_ADDR", "")
	t.Setenv("GRPC_ADDR", "")
	if _, err := loadConfig(); err == nil || err.Error() != "VERSION_ADDR is required" {
		t.Fatalf("err = %v", err)
	}

	t.Setenv("VERSION_ADDR", "127.0.0.1:1")
	cfg, err := loadConfig()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.grpcAddr != ":9092" || cfg.versionAddr != "127.0.0.1:1" {
		t.Fatalf("cfg = %+v", cfg)
	}
}
