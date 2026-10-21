// Package dag is the pure node math of design §2: a node names the state of
// one document log prefix, and its ID hashes that location, the content hash
// and the parents. Metadata stays out of the hash.
package dag

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"

	"github.com/google/uuid"
)

type NodeID [32]byte

type Hash [32]byte

func (id NodeID) String() string { return hex.EncodeToString(id[:]) }

func (h Hash) String() string { return hex.EncodeToString(h[:]) }

// Node is one log prefix. Parents is empty for a root document's genesis,
// [base] for a fork's first node and [prev] for any later node; the first
// parent is the document's own line.
type Node struct {
	Doc     uuid.UUID
	Seq     uint64
	Content Hash
	Parents []NodeID
}

const idMagic = "CSV1"

// ID is SHA-256("CSV1" | doc uuid[16] | uvarint(seq) | content_hash[32] |
// uvarint(len(parents)) | parent_id[32]...).
func (n Node) ID() NodeID {
	b := make([]byte, 0, len(idMagic)+16+binary.MaxVarintLen64+32+1+32*len(n.Parents))
	b = append(b, idMagic...)
	b = append(b, n.Doc[:]...)
	b = binary.AppendUvarint(b, n.Seq)
	b = append(b, n.Content[:]...)
	b = binary.AppendUvarint(b, uint64(len(n.Parents)))
	for _, p := range n.Parents {
		b = append(b, p[:]...)
	}
	return sha256.Sum256(b)
}

func ContentHash(canonical []byte) Hash { return sha256.Sum256(canonical) }
