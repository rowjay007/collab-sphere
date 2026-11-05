// Package docrypt seals document runes and wraps document keys.
// Services do not import it.
package docrypt

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/ecdh"
	"crypto/hkdf"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"unicode/utf8"

	"github.com/google/uuid"
	"github.com/rowjay007/collab-sphere/pkg/crdt"
)

var (
	ErrKey  = errors.New("docrypt: key")
	ErrOpen = errors.New("docrypt: open")
	ErrRune = errors.New("docrypt: rune")
	ErrWrap = errors.New("docrypt: wrap")
)

// Wrap is a document key sealed to one member.
type Wrap struct {
	EphPub     []byte
	Nonce      []byte
	Ciphertext []byte
}

// Nonce is the first 12 bytes of SHA-256 over the document id, the epoch,
// and the lineage root. A copy passes the original character as root.
func Nonce(doc uuid.UUID, epoch uint32, root crdt.ID) []byte {
	sum := sha256.Sum256(material(doc, epoch, root))
	out := make([]byte, 12)
	copy(out, sum[:12])
	return out
}

// Seal encrypts one rune. op is the insert id. root is the lineage root.
// They are equal for an ordinary insert. The nonce is derived from root.
func Seal(key []byte, doc uuid.UUID, epoch uint32, op, root crdt.ID, r rune) ([]byte, error) {
	return seal(key, doc, epoch, lineage(op, root), r)
}

func lineage(op, root crdt.ID) crdt.ID { return root }

// Open decrypts a rune sealed under root.
func Open(key []byte, doc uuid.UUID, epoch uint32, root crdt.ID, ciphertext []byte) (rune, error) {
	block, err := aesKey(key)
	if err != nil {
		return 0, err
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return 0, err
	}
	plain, err := gcm.Open(nil, Nonce(doc, epoch, root), ciphertext, sealAD(doc, epoch, root))
	if err != nil {
		return 0, fmt.Errorf("%w: %v", ErrOpen, err)
	}
	if !utf8.Valid(plain) || utf8.RuneCount(plain) != 1 {
		return 0, ErrRune
	}
	r, size := utf8.DecodeRune(plain)
	if size != len(plain) {
		return 0, ErrRune
	}
	return r, nil
}

// WrapKey seals a 32-byte document key to member.
func WrapKey(docKey []byte, doc uuid.UUID, epoch uint32, user string, keyID uuid.UUID, member *ecdh.PublicKey) (Wrap, error) {
	if len(docKey) != 32 {
		return Wrap{}, fmt.Errorf("%w: document key must be 32 bytes", ErrKey)
	}
	if member == nil {
		return Wrap{}, fmt.Errorf("%w: missing public key", ErrWrap)
	}
	eph, err := ecdh.X25519().GenerateKey(rand.Reader)
	if err != nil {
		return Wrap{}, err
	}
	shared, err := eph.ECDH(member)
	if err != nil {
		return Wrap{}, err
	}
	wrapKey, err := hkdf.Key(sha256.New, shared, doc[:], string(wrapInfo(epoch, user)), 32)
	if err != nil {
		return Wrap{}, err
	}
	block, err := aes.NewCipher(wrapKey)
	if err != nil {
		return Wrap{}, err
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return Wrap{}, err
	}
	nonce := make([]byte, gcm.NonceSize())
	if _, err := rand.Read(nonce); err != nil {
		return Wrap{}, err
	}
	return Wrap{
		EphPub:     append([]byte(nil), eph.PublicKey().Bytes()...),
		Nonce:      nonce,
		Ciphertext: gcm.Seal(nil, nonce, docKey, wrapAD(doc, epoch, user, keyID)),
	}, nil
}

// UnwrapKey opens a wrap with the member's private key.
func UnwrapKey(w Wrap, doc uuid.UUID, epoch uint32, user string, keyID uuid.UUID, member *ecdh.PrivateKey) ([]byte, error) {
	if member == nil {
		return nil, fmt.Errorf("%w: missing private key", ErrWrap)
	}
	ephPub, err := ecdh.X25519().NewPublicKey(w.EphPub)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrWrap, err)
	}
	shared, err := member.ECDH(ephPub)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrWrap, err)
	}
	wrapKey, err := hkdf.Key(sha256.New, shared, doc[:], string(wrapInfo(epoch, user)), 32)
	if err != nil {
		return nil, err
	}
	block, err := aes.NewCipher(wrapKey)
	if err != nil {
		return nil, err
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}
	plain, err := gcm.Open(nil, w.Nonce, w.Ciphertext, wrapAD(doc, epoch, user, keyID))
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrOpen, err)
	}
	if len(plain) != 32 {
		return nil, fmt.Errorf("%w: document key must be 32 bytes", ErrKey)
	}
	return plain, nil
}

func seal(key []byte, doc uuid.UUID, epoch uint32, root crdt.ID, r rune) ([]byte, error) {
	if !utf8.ValidRune(r) {
		return nil, ErrRune
	}
	block, err := aesKey(key)
	if err != nil {
		return nil, err
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}
	var plain [utf8.UTFMax]byte
	n := utf8.EncodeRune(plain[:], r)
	return gcm.Seal(nil, Nonce(doc, epoch, root), plain[:n], sealAD(doc, epoch, root)), nil
}

func aesKey(key []byte) (cipher.Block, error) {
	if len(key) != 32 {
		return nil, fmt.Errorf("%w: AES-256 key must be 32 bytes", ErrKey)
	}
	return aes.NewCipher(key)
}

func material(doc uuid.UUID, epoch uint32, root crdt.ID) []byte {
	b := make([]byte, 0, 16+4+8+8)
	b = append(b, doc[:]...)
	b = binary.BigEndian.AppendUint32(b, epoch)
	b = binary.BigEndian.AppendUint64(b, uint64(root.Client))
	b = binary.BigEndian.AppendUint64(b, root.Clock)
	return b
}

func sealAD(doc uuid.UUID, epoch uint32, root crdt.ID) []byte {
	return append([]byte("CSAD1"), material(doc, epoch, root)...)
}

func wrapInfo(epoch uint32, user string) []byte {
	b := binary.BigEndian.AppendUint32([]byte("collab-wrap-v1"), epoch)
	return append(b, user...)
}

func wrapAD(doc uuid.UUID, epoch uint32, user string, keyID uuid.UUID) []byte {
	b := make([]byte, 0, 16+4+len(user)+16)
	b = append(b, doc[:]...)
	b = binary.BigEndian.AppendUint32(b, epoch)
	b = append(b, user...)
	return append(b, keyID[:]...)
}
