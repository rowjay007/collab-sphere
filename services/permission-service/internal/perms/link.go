package perms

import (
	"crypto/ed25519"
	"encoding/base64"
	"encoding/binary"
	"strings"
	"time"

	"github.com/google/uuid"
)

const (
	linkPrefix = "s1."
	linkMagic  = "CSL1"
)

func roleByte(r Role) (byte, bool) {
	switch r {
	case RoleEditor:
		return 2, true
	case RoleCommenter:
		return 3, true
	case RoleViewer:
		return 4, true
	default:
		return 0, false
	}
}

func roleFromByte(b byte) (Role, bool) {
	switch b {
	case 2:
		return RoleEditor, true
	case 3:
		return RoleCommenter, true
	case 4:
		return RoleViewer, true
	default:
		return "", false
	}
}

func signLink(priv ed25519.PrivateKey, id, doc uuid.UUID, role Role, exp time.Time) (string, error) {
	payload, err := encodeLink(id, doc, role, exp)
	if err != nil {
		return "", err
	}
	sig := ed25519.Sign(priv, payload)
	return linkPrefix + base64.RawURLEncoding.EncodeToString(payload) + "." + base64.RawURLEncoding.EncodeToString(sig), nil
}

func parseLink(pub ed25519.PublicKey, token string) (uuid.UUID, uuid.UUID, Role, time.Time, error) {
	if !strings.HasPrefix(token, linkPrefix) {
		return uuid.Nil, uuid.Nil, "", time.Time{}, ErrLinkBad
	}
	rest := token[len(linkPrefix):]
	payloadB64, sigB64, ok := strings.Cut(rest, ".")
	if !ok || payloadB64 == "" || sigB64 == "" || strings.Contains(sigB64, ".") {
		return uuid.Nil, uuid.Nil, "", time.Time{}, ErrLinkBad
	}
	payload, err := base64.RawURLEncoding.DecodeString(payloadB64)
	if err != nil {
		return uuid.Nil, uuid.Nil, "", time.Time{}, ErrLinkBad
	}
	sig, err := base64.RawURLEncoding.DecodeString(sigB64)
	if err != nil || len(sig) != ed25519.SignatureSize {
		return uuid.Nil, uuid.Nil, "", time.Time{}, ErrLinkBad
	}
	if base64.RawURLEncoding.EncodeToString(payload) != payloadB64 || base64.RawURLEncoding.EncodeToString(sig) != sigB64 {
		return uuid.Nil, uuid.Nil, "", time.Time{}, ErrLinkBad
	}
	if !ed25519.Verify(pub, payload, sig) {
		return uuid.Nil, uuid.Nil, "", time.Time{}, ErrLinkBad
	}
	return decodeLink(payload)
}

func encodeLink(id, doc uuid.UUID, role Role, exp time.Time) ([]byte, error) {
	rb, ok := roleByte(role)
	if !ok {
		return nil, ErrInvalidArgument
	}
	buf := make([]byte, 4+16+16+1+8)
	copy(buf[:4], linkMagic)
	copy(buf[4:20], id[:])
	copy(buf[20:36], doc[:])
	buf[36] = rb
	binary.BigEndian.PutUint64(buf[37:], uint64(exp.Unix()))
	return buf, nil
}

func decodeLink(p []byte) (uuid.UUID, uuid.UUID, Role, time.Time, error) {
	if len(p) != 45 || string(p[:4]) != linkMagic {
		return uuid.Nil, uuid.Nil, "", time.Time{}, ErrLinkBad
	}
	var id, doc uuid.UUID
	copy(id[:], p[4:20])
	copy(doc[:], p[20:36])
	role, ok := roleFromByte(p[36])
	if !ok {
		return uuid.Nil, uuid.Nil, "", time.Time{}, ErrLinkBad
	}
	exp := time.Unix(int64(binary.BigEndian.Uint64(p[37:])), 0).UTC()
	return id, doc, role, exp, nil
}
