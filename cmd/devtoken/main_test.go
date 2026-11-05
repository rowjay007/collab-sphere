package main

import (
	"bytes"
	"strings"
	"testing"
	"time"

	"github.com/rowjay007/collab-sphere/pkg/auth"
)

func TestGenAndMint(t *testing.T) {
	var keys bytes.Buffer
	if err := run([]string{"-gen"}, &keys, ""); err != nil {
		t.Fatal(err)
	}
	pub, priv := parseGen(t, keys.String())
	var out bytes.Buffer
	if err := run([]string{"-subject", "ivy", "-ttl", "1h"}, &out, priv); err != nil {
		t.Fatal(err)
	}
	tok := strings.TrimSpace(out.String())
	got, err := auth.Verify(mustPub(t, pub), tok, time.Now())
	if err != nil || got != "ivy" {
		t.Fatalf("verify %q: %v", got, err)
	}
}

func TestMintRequiresSubjectAndKey(t *testing.T) {
	if err := run(nil, &bytes.Buffer{}, "x"); err == nil || !strings.Contains(err.Error(), "subject") {
		t.Fatalf("err = %v", err)
	}
	if err := run([]string{"-subject", "ivy"}, &bytes.Buffer{}, ""); err == nil || !strings.Contains(err.Error(), "AUTH_PRIVATE_KEY") {
		t.Fatalf("err = %v", err)
	}
}

func parseGen(t *testing.T, s string) (pub, priv string) {
	t.Helper()
	for _, line := range strings.Split(s, "\n") {
		k, v, ok := strings.Cut(line, "=")
		if !ok {
			continue
		}
		switch k {
		case "AUTH_PUBLIC_KEY":
			pub = v
		case "AUTH_PRIVATE_KEY":
			priv = v
		}
	}
	if pub == "" || priv == "" {
		t.Fatalf("gen output %q", s)
	}
	return pub, priv
}

func mustPub(t *testing.T, b64 string) []byte {
	t.Helper()
	pub, err := auth.ParsePublicKey(b64)
	if err != nil {
		t.Fatal(err)
	}
	return pub
}
