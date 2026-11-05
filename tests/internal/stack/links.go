package stack

import (
	"context"
	"crypto/ed25519"
	"encoding/base64"
	encbin "encoding/binary"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/rowjay007/collab-sphere/pkg/auth"
)

const (
	linkPrefix = "s1."
	linkMagic  = "CSL1"
	viewerByte = 4
	linkBytes  = 45
)

func (p *PermissionService) shareKey() ed25519.PrivateKey {
	p.s.t.Helper()
	priv, err := auth.ParsePrivateKey(p.sharePriv)
	if err != nil {
		p.s.t.Fatal(err)
	}
	return priv
}

// ExpiredShareToken inserts a share_links row whose expires_at is already past
// and returns a token signed over those stored fields. The running
// permission-service process has no clock hook, so expiry cannot be created
// through CreateLink without waiting out the one-minute minimum TTL.
func (p *PermissionService) ExpiredShareToken(ctx context.Context, docID, createdBy string) string {
	p.s.t.Helper()
	doc, err := uuid.Parse(docID)
	if err != nil {
		p.s.t.Fatal(err)
	}
	id := uuid.New()
	exp := time.Now().Add(-time.Minute).UTC().Truncate(time.Second)
	if _, err := p.db.Exec(ctx, `
		INSERT INTO share_links (id, doc_id, role, expires_at, created_by)
		VALUES ($1, $2, 'viewer', $3, $4)`, id, doc, exp, createdBy); err != nil {
		p.s.t.Fatal(err)
	}
	return signShare(p.shareKey(), id, doc, viewerByte, exp)
}

// SwapLinkDoc re-signs token with a different document uuid so Redeem sees a
// field mismatch rather than a bad signature.
func (p *PermissionService) SwapLinkDoc(token, docID string) string {
	p.s.t.Helper()
	doc, err := uuid.Parse(docID)
	if err != nil {
		p.s.t.Fatal(err)
	}
	payload := decodeLinkPayload(p.s.t, token)
	copy(payload[20:36], doc[:])
	sig := ed25519.Sign(p.shareKey(), payload)
	return linkPrefix + base64.RawURLEncoding.EncodeToString(payload) + "." + base64.RawURLEncoding.EncodeToString(sig)
}

func decodeLinkPayload(t *testing.T, token string) []byte {
	t.Helper()
	rest, ok := strings.CutPrefix(token, linkPrefix)
	if !ok {
		t.Fatalf("token prefix: %q", token)
	}
	payloadB64, _, ok := strings.Cut(rest, ".")
	if !ok {
		t.Fatalf("token form: %q", token)
	}
	payload, err := base64.RawURLEncoding.DecodeString(payloadB64)
	if err != nil || len(payload) != linkBytes {
		t.Fatalf("payload: %v len %d", err, len(payload))
	}
	return payload
}

func signShare(priv ed25519.PrivateKey, id, doc uuid.UUID, role byte, exp time.Time) string {
	buf := make([]byte, linkBytes)
	copy(buf[:4], linkMagic)
	copy(buf[4:20], id[:])
	copy(buf[20:36], doc[:])
	buf[36] = role
	encbin.BigEndian.PutUint64(buf[37:], uint64(exp.Unix()))
	sig := ed25519.Sign(priv, buf)
	return linkPrefix + base64.RawURLEncoding.EncodeToString(buf) + "." + base64.RawURLEncoding.EncodeToString(sig)
}
