package main

import "testing"

func TestLoadConfigRequiresEnv(t *testing.T) {
	t.Setenv("DATABASE_URL", "postgres://x")
	t.Setenv("AUTH_PUBLIC_KEY", "pub")
	t.Setenv("SHARE_PRIVATE_KEY", "share")
	t.Setenv("INTERNAL_TOKEN", "tok")
	t.Setenv("DOCUMENT_ADDR", "")
	t.Setenv("GRPC_ADDR", "")
	if _, err := loadConfig(); err == nil || err.Error() != "DOCUMENT_ADDR is required" {
		t.Fatalf("err = %v", err)
	}
	t.Setenv("DOCUMENT_ADDR", "127.0.0.1:9090")
	cfg, err := loadConfig()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.grpcAddr != ":9094" || cfg.documentAddr != "127.0.0.1:9090" {
		t.Fatalf("cfg = %+v", cfg)
	}
}
