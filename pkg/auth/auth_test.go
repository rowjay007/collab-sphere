package auth_test

import (
	"crypto/ed25519"
	"encoding/base64"
	"strings"
	"testing"
	"time"

	"github.com/rowjay007/collab-sphere/pkg/auth"
)

func TestMintVerify(t *testing.T) {
	pub, priv := mustKey(t)
	now := time.Date(2026, 11, 5, 12, 0, 0, 0, time.UTC)
	tok, err := auth.Mint(priv, "alice", now, now.Add(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	got, err := auth.Verify(pub, tok, now)
	if err != nil || got != "alice" {
		t.Fatalf("verify: %q %v", got, err)
	}
}

func TestMintRejectsBadHandle(t *testing.T) {
	_, priv := mustKey(t)
	now := time.Now()
	for _, h := range []string{"", "Alice", "-x", ".x", "x/y", strings.Repeat("a", 65)} {
		if _, err := auth.Mint(priv, h, now, now.Add(time.Hour)); err != auth.ErrInvalidHandle {
			t.Fatalf("handle %q: %v", h, err)
		}
	}
}

func TestValidHandle(t *testing.T) {
	ok := []string{"a", "z9", "a.b", "a_b", "a-b", "user.name_1", strings.Repeat("x", 64)}
	for _, h := range ok {
		if !auth.ValidHandle(h) {
			t.Fatalf("rejected %q", h)
		}
	}
	bad := []string{"", "A", "-a", ".a", "_a", "a/b", "a@b", " a", "a ", strings.Repeat("x", 65)}
	for _, h := range bad {
		if auth.ValidHandle(h) {
			t.Fatalf("accepted %q", h)
		}
	}
}

func TestExpiredBeyondSkew(t *testing.T) {
	pub, priv := mustKey(t)
	now := time.Date(2026, 11, 5, 12, 0, 0, 0, time.UTC)
	tok, err := auth.Mint(priv, "bob", now.Add(-2*time.Hour), now.Add(-time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := auth.Verify(pub, tok, now); err != auth.ErrExpired {
		t.Fatalf("err = %v", err)
	}
}

func TestSkewAllowsThirtySeconds(t *testing.T) {
	pub, priv := mustKey(t)
	issued := time.Date(2026, 11, 5, 12, 0, 0, 0, time.UTC)
	exp := issued.Add(time.Minute)
	tok, err := auth.Mint(priv, "carol", issued, exp)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := auth.Verify(pub, tok, exp.Add(30*time.Second)); err != nil {
		t.Fatalf("at +30s: %v", err)
	}
	if _, err := auth.Verify(pub, tok, exp.Add(31*time.Second)); err != auth.ErrExpired {
		t.Fatalf("at +31s: %v", err)
	}
	if _, err := auth.Verify(pub, tok, issued.Add(-30*time.Second)); err != nil {
		t.Fatalf("at -30s: %v", err)
	}
	if _, err := auth.Verify(pub, tok, issued.Add(-31*time.Second)); err != auth.ErrNotYetValid {
		t.Fatalf("at -31s: %v", err)
	}
}

func TestTruncation(t *testing.T) {
	pub, priv := mustKey(t)
	now := time.Now()
	tok, err := auth.Mint(priv, "dave", now, now.Add(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < len(tok); i++ {
		if _, err := auth.Verify(pub, tok[:i], now); err == nil {
			t.Fatalf("accepted truncation at %d", i)
		}
	}
}

func TestEveryBitFlipFails(t *testing.T) {
	pub, priv := mustKey(t)
	now := time.Now()
	tok, err := auth.Mint(priv, "erin", now, now.Add(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	raw := []byte(tok)
	for i := 0; i < len(raw)*8; i++ {
		flip := append([]byte(nil), raw...)
		flip[i/8] ^= 1 << (i % 8)
		if _, err := auth.Verify(pub, string(flip), now); err == nil {
			t.Fatalf("accepted bit %d", i)
		}
	}
}

func TestWrongScheme(t *testing.T) {
	pub, priv := mustKey(t)
	now := time.Now()
	tok, err := auth.Mint(priv, "frank", now, now.Add(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	body := strings.TrimPrefix(tok, "v1.")
	for _, bad := range []string{
		"s1." + body,
		"v2." + body,
		"internal." + strings.Repeat("ab", 32),
		"Bearer " + tok,
		tok[3:],
	} {
		if _, err := auth.Verify(pub, bad, now); err != auth.ErrWrongScheme && err != auth.ErrInvalidToken {
			t.Fatalf("%q: %v", bad, err)
		}
		if _, err := auth.Verify(pub, bad, now); err != auth.ErrWrongScheme && !strings.HasPrefix(bad, "v") {
			if err != auth.ErrWrongScheme {
				t.Fatalf("scheme %q: %v", bad, err)
			}
		}
	}
	if _, err := auth.Verify(pub, "s1."+body, now); err != auth.ErrWrongScheme {
		t.Fatalf("s1: %v", err)
	}
	if _, err := auth.Verify(pub, "internal."+strings.Repeat("ab", 32), now); err != auth.ErrWrongScheme {
		t.Fatalf("internal: %v", err)
	}
}

func TestParseKeys(t *testing.T) {
	pub, priv := mustKey(t)
	gotPub, err := auth.ParsePublicKey(auth.EncodeKey(pub))
	if err != nil || !gotPub.Equal(pub) {
		t.Fatalf("pub: %v %v", gotPub, err)
	}
	gotPriv, err := auth.ParsePrivateKey(auth.EncodeKey(priv))
	if err != nil || !gotPriv.Equal(priv) {
		t.Fatalf("priv: %v", err)
	}
	seed := priv.Seed()
	fromSeed, err := auth.ParsePrivateKey(auth.EncodeKey(seed))
	if err != nil || !fromSeed.Equal(priv) {
		t.Fatalf("seed: %v", err)
	}
	if _, err := auth.ParsePublicKey("????"); err != auth.ErrInvalidKey {
		t.Fatalf("bad pub: %v", err)
	}
	if _, err := auth.ParsePrivateKey(base64.StdEncoding.EncodeToString([]byte("short"))); err != auth.ErrInvalidKey {
		t.Fatalf("short priv: %v", err)
	}
}

func TestMatchInternal(t *testing.T) {
	secret := strings.Repeat("ab", 32)
	tok := auth.FormatInternal(secret)
	if !auth.MatchInternal(tok, secret) || !auth.MatchInternal(tok, tok) {
		t.Fatal("expected match")
	}
	if auth.MatchInternal(auth.FormatInternal(strings.Repeat("cd", 32)), secret) {
		t.Fatal("matched wrong secret")
	}
	if auth.MatchInternal("v1.abc.def", secret) {
		t.Fatal("matched user token")
	}
	if _, err := auth.ParseInternal("nothex"); err != auth.ErrInvalidSecret {
		t.Fatalf("parse: %v", err)
	}
}

func mustKey(t *testing.T) (ed25519.PublicKey, ed25519.PrivateKey) {
	t.Helper()
	pub, priv, err := auth.GenerateKeypair()
	if err != nil {
		t.Fatal(err)
	}
	return pub, priv
}
