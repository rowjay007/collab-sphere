package auth

import (
	"crypto/ed25519"
	"crypto/subtle"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"time"
)

const (
	tokenPrefix    = "v1."
	internalPrefix = "internal."
	internalHexLen = 64
	skew           = 30 * time.Second
)

var (
	ErrInvalidHandle = errors.New("auth: invalid handle")
	ErrInvalidToken  = errors.New("auth: invalid token")
	ErrExpired       = errors.New("auth: expired")
	ErrNotYetValid   = errors.New("auth: not yet valid")
	ErrWrongScheme   = errors.New("auth: wrong scheme")
	ErrInvalidKey    = errors.New("auth: invalid key")
	ErrInvalidSecret = errors.New("auth: invalid internal token")
)

var handleRE = regexp.MustCompile(`^[a-z0-9][a-z0-9._-]{0,63}$`)

func ValidHandle(s string) bool {
	return handleRE.MatchString(s)
}

func ParsePublicKey(b64 string) (ed25519.PublicKey, error) {
	raw, err := base64.StdEncoding.DecodeString(b64)
	if err != nil || len(raw) != ed25519.PublicKeySize {
		return nil, ErrInvalidKey
	}
	return ed25519.PublicKey(raw), nil
}

func ParsePrivateKey(b64 string) (ed25519.PrivateKey, error) {
	raw, err := base64.StdEncoding.DecodeString(b64)
	if err != nil {
		return nil, ErrInvalidKey
	}
	switch len(raw) {
	case ed25519.SeedSize:
		return ed25519.NewKeyFromSeed(raw), nil
	case ed25519.PrivateKeySize:
		return ed25519.PrivateKey(raw), nil
	default:
		return nil, ErrInvalidKey
	}
}

func EncodeKey(key []byte) string {
	return base64.StdEncoding.EncodeToString(key)
}

func ParseInternal(s string) (string, error) {
	secret := strings.TrimPrefix(s, internalPrefix)
	if len(secret) != internalHexLen || !isHex(secret) {
		return "", ErrInvalidSecret
	}
	return secret, nil
}

func FormatInternal(secret string) string {
	return internalPrefix + secret
}

func Mint(priv ed25519.PrivateKey, subject string, issuedAt, expiry time.Time) (string, error) {
	if !ValidHandle(subject) {
		return "", ErrInvalidHandle
	}
	if priv == nil || len(priv) != ed25519.PrivateKeySize {
		return "", ErrInvalidKey
	}
	payload, err := encodePayload(subject, issuedAt.Unix(), expiry.Unix())
	if err != nil {
		return "", err
	}
	sig := ed25519.Sign(priv, payload)
	return tokenPrefix + b64url(payload) + "." + b64url(sig), nil
}

func Verify(pub ed25519.PublicKey, token string, now time.Time) (string, error) {
	if pub == nil || len(pub) != ed25519.PublicKeySize {
		return "", ErrInvalidKey
	}
	if strings.HasPrefix(token, internalPrefix) {
		return "", ErrWrongScheme
	}
	if !strings.HasPrefix(token, tokenPrefix) {
		return "", ErrWrongScheme
	}
	rest := token[len(tokenPrefix):]
	payloadB64, sigB64, ok := strings.Cut(rest, ".")
	if !ok || payloadB64 == "" || sigB64 == "" || strings.Contains(sigB64, ".") {
		return "", ErrInvalidToken
	}
	payload, err := unb64url(payloadB64)
	if err != nil {
		return "", ErrInvalidToken
	}
	sig, err := unb64url(sigB64)
	if err != nil || len(sig) != ed25519.SignatureSize {
		return "", ErrInvalidToken
	}
	if b64url(payload) != payloadB64 || b64url(sig) != sigB64 {
		return "", ErrInvalidToken
	}
	if !ed25519.Verify(pub, payload, sig) {
		return "", ErrInvalidToken
	}
	subject, iat, exp, err := decodePayload(payload)
	if err != nil {
		return "", err
	}
	if !ValidHandle(subject) {
		return "", ErrInvalidHandle
	}
	if now.Add(skew).Before(time.Unix(iat, 0)) {
		return "", ErrNotYetValid
	}
	if now.Add(-skew).After(time.Unix(exp, 0)) {
		return "", ErrExpired
	}
	return subject, nil
}

func MatchInternal(presented, expected string) bool {
	want, err := ParseInternal(expected)
	if err != nil {
		return false
	}
	got, err := ParseInternal(presented)
	if err != nil {
		subtle.ConstantTimeCompare([]byte(want), []byte(want))
		return false
	}
	return subtle.ConstantTimeCompare([]byte(got), []byte(want)) == 1
}

func encodePayload(subject string, iat, exp int64) ([]byte, error) {
	if len(subject) > 0xffff {
		return nil, ErrInvalidHandle
	}
	buf := make([]byte, 2+len(subject)+16)
	binary.BigEndian.PutUint16(buf[:2], uint16(len(subject)))
	copy(buf[2:], subject)
	binary.BigEndian.PutUint64(buf[2+len(subject):], uint64(iat))
	binary.BigEndian.PutUint64(buf[2+len(subject)+8:], uint64(exp))
	return buf, nil
}

func decodePayload(p []byte) (string, int64, int64, error) {
	if len(p) < 18 {
		return "", 0, 0, ErrInvalidToken
	}
	n := int(binary.BigEndian.Uint16(p[:2]))
	if n < 1 || 2+n+16 != len(p) {
		return "", 0, 0, ErrInvalidToken
	}
	subject := string(p[2 : 2+n])
	iat := int64(binary.BigEndian.Uint64(p[2+n : 2+n+8]))
	exp := int64(binary.BigEndian.Uint64(p[2+n+8 : 2+n+16]))
	return subject, iat, exp, nil
}

func b64url(b []byte) string {
	return base64.RawURLEncoding.EncodeToString(b)
}

func unb64url(s string) ([]byte, error) {
	return base64.RawURLEncoding.DecodeString(s)
}

func isHex(s string) bool {
	if len(s) == 0 || len(s)%2 != 0 {
		return false
	}
	for i := 0; i < len(s); i++ {
		c := s[i]
		if c >= '0' && c <= '9' || c >= 'a' && c <= 'f' || c >= 'A' && c <= 'F' {
			continue
		}
		return false
	}
	return true
}

func GenerateKeypair() (ed25519.PublicKey, ed25519.PrivateKey, error) {
	pub, priv, err := ed25519.GenerateKey(nil)
	if err != nil {
		return nil, nil, fmt.Errorf("auth: generate key: %w", err)
	}
	return pub, priv, nil
}
