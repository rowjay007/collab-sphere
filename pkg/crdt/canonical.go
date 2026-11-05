package crdt

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"
	"sort"

	crdtv1 "github.com/rowjay007/collab-sphere/gen/crdt/v1"
)

const (
	canonicalMagic  = "CSS1"
	canonicalMagic2 = "CSS2"
)

const flagCopy = 1

// CanonicalBytes encodes d in the hashed snapshot format that ADR 0010
// specifies byte for byte. A document with pending operations has no
// canonical form and returns ErrPending.
func CanonicalBytes(d *Doc) ([]byte, error) {
	if d == nil {
		return nil, fmt.Errorf("%w: nil document", ErrInvalidOp)
	}
	if d.mode == Encrypted {
		return nil, ErrEncrypted
	}
	if len(d.pendingInserts) != 0 || len(d.pendingDeletes) != 0 {
		return nil, ErrPending
	}
	ids := d.nodeIDsDFS()
	b := make([]byte, 0, len(canonicalMagic)+16*len(ids))
	b = append(b, canonicalMagic...)
	b = binary.AppendUvarint(b, uint64(len(ids)))
	for _, id := range ids {
		n := d.nodes[id]
		b = appendCanonicalID(b, n.id)
		b = appendCanonicalID(b, n.parent)
		b = binary.AppendUvarint(b, uint64(n.value))
		var flags uint64
		if n.copy {
			flags |= flagCopy
		}
		b = binary.AppendUvarint(b, flags)
		b = binary.AppendUvarint(b, uint64(len(n.deletedBy)))
		for _, deleteID := range n.deletedBy {
			b = appendCanonicalID(b, deleteID)
		}
	}
	b = appendCanonicalVector(b, d.vector)
	b = appendCanonicalVector(b, d.collected)
	return b, nil
}

// CanonicalCSS2 encodes an encrypted document. The layout is the phase 4
// CSS2 rule. A plain document has no CSS2 form.
func CanonicalCSS2(d *Doc) ([]byte, error) {
	if d == nil {
		return nil, fmt.Errorf("%w: nil document", ErrInvalidOp)
	}
	if d.mode != Encrypted {
		return nil, fmt.Errorf("%w: CSS2 requires an encrypted document", ErrInvalidOp)
	}
	if len(d.pendingInserts) != 0 || len(d.pendingDeletes) != 0 {
		return nil, ErrPending
	}
	ids := d.nodeIDsDFS()
	b := make([]byte, 0, len(canonicalMagic2)+24*len(ids))
	b = append(b, canonicalMagic2...)
	b = binary.AppendUvarint(b, uint64(len(ids)))
	for _, id := range ids {
		n := d.nodes[id]
		if n.cipher == nil {
			return nil, fmt.Errorf("%w: encrypted node lacks a cipher", ErrInvalidOp)
		}
		b = appendCanonicalID(b, n.id)
		b = appendCanonicalID(b, n.parent)
		b = binary.AppendUvarint(b, uint64(n.cipher.Epoch))
		b = binary.AppendUvarint(b, uint64(len(n.cipher.Bytes)))
		b = append(b, n.cipher.Bytes...)
		var flags uint64
		if n.copy {
			flags |= flagCopy
		}
		b = binary.AppendUvarint(b, flags)
		b = binary.AppendUvarint(b, uint64(len(n.deletedBy)))
		for _, deleteID := range n.deletedBy {
			b = appendCanonicalID(b, deleteID)
		}
	}
	b = appendCanonicalVector(b, d.vector)
	b = appendCanonicalVector(b, d.collected)
	return b, nil
}

// FromCanonical decodes CanonicalBytes or CanonicalCSS2 output. It dispatches
// on the magic, re-encodes the result, and requires byte equality, so every
// state has exactly one accepted encoding.
func FromCanonical(b []byte) (*Doc, error) {
	if bytes.HasPrefix(b, []byte(canonicalMagic2)) {
		return fromCanonicalCSS2(b)
	}
	return fromCanonicalCSS1(b)
}

func fromCanonicalCSS1(b []byte) (*Doc, error) {
	r := canonicalReader{b: b}
	if !bytes.HasPrefix(b, []byte(canonicalMagic)) {
		return nil, fmt.Errorf("%w: canonical snapshot lacks %q", ErrInvalidOp, canonicalMagic)
	}
	r.b = r.b[len(canonicalMagic):]
	snapshot := &crdtv1.Snapshot{}
	nodes := r.count(7)
	for range nodes {
		if r.err != nil {
			break
		}
		pbNode := &crdtv1.Node{Id: r.id(), Parent: r.id()}
		value := r.uvarint()
		if value > math.MaxUint32 {
			r.fail("value overflows a rune")
		}
		pbNode.Value = uint32(value)
		switch flags := r.uvarint(); flags {
		case 0:
		case flagCopy:
			pbNode.Copy = true
		default:
			r.fail(fmt.Sprintf("unknown node flags %d", flags))
		}
		deletes := r.count(2)
		for range deletes {
			pbNode.DeletedBy = append(pbNode.DeletedBy, r.id())
		}
		snapshot.Nodes = append(snapshot.Nodes, pbNode)
	}
	snapshot.Vector = r.vector()
	snapshot.Collected = r.vector()
	if r.err == nil && len(r.b) != 0 {
		r.fail("trailing bytes")
	}
	if r.err != nil {
		return nil, r.err
	}
	d, err := FromSnapshot(snapshot)
	if err != nil {
		return nil, err
	}
	again, err := CanonicalBytes(d)
	if err != nil {
		return nil, err
	}
	if !bytes.Equal(again, b) {
		return nil, fmt.Errorf("%w: snapshot is not in canonical form", ErrInvalidOp)
	}
	return d, nil
}

func fromCanonicalCSS2(b []byte) (*Doc, error) {
	r := canonicalReader{b: b[len(canonicalMagic2):]}
	snapshot := &crdtv1.Snapshot{Encrypted: true}
	nodes := r.count(8)
	for range nodes {
		if r.err != nil {
			break
		}
		pbNode := &crdtv1.Node{Id: r.id(), Parent: r.id()}
		epoch := r.uvarint()
		if epoch > math.MaxUint32 {
			r.fail("epoch overflows uint32")
		}
		pbNode.Epoch = uint32(epoch)
		pbNode.Cipher = r.raw(r.uvarint())
		switch flags := r.uvarint(); flags {
		case 0:
		case flagCopy:
			pbNode.Copy = true
		default:
			r.fail(fmt.Sprintf("unknown node flags %d", flags))
		}
		deletes := r.count(2)
		for range deletes {
			pbNode.DeletedBy = append(pbNode.DeletedBy, r.id())
		}
		snapshot.Nodes = append(snapshot.Nodes, pbNode)
	}
	snapshot.Vector = r.vector()
	snapshot.Collected = r.vector()
	if r.err == nil && len(r.b) != 0 {
		r.fail("trailing bytes")
	}
	if r.err != nil {
		return nil, r.err
	}
	d, err := FromSnapshot(snapshot)
	if err != nil {
		return nil, err
	}
	again, err := CanonicalCSS2(d)
	if err != nil {
		return nil, err
	}
	if !bytes.Equal(again, b) {
		return nil, fmt.Errorf("%w: snapshot is not in canonical form", ErrInvalidOp)
	}
	return d, nil
}

func appendCanonicalID(b []byte, id ID) []byte {
	b = binary.AppendUvarint(b, uint64(id.Client))
	return binary.AppendUvarint(b, id.Clock)
}

func appendCanonicalVector(b []byte, vector VersionVector) []byte {
	clients := make([]ClientID, 0, len(vector))
	for client, clock := range vector {
		if client != 0 && clock != 0 {
			clients = append(clients, client)
		}
	}
	sort.Slice(clients, func(i, j int) bool { return clients[i] < clients[j] })
	b = binary.AppendUvarint(b, uint64(len(clients)))
	for _, client := range clients {
		b = binary.AppendUvarint(b, uint64(client))
		b = binary.AppendUvarint(b, vector[client])
	}
	return b
}

type canonicalReader struct {
	b   []byte
	err error
}

func (r *canonicalReader) fail(reason string) {
	if r.err == nil {
		r.err = fmt.Errorf("%w: canonical snapshot: %s", ErrInvalidOp, reason)
	}
}

func (r *canonicalReader) raw(n uint64) []byte {
	if r.err != nil {
		return nil
	}
	if n > uint64(len(r.b)) {
		r.fail("cipher exceeds remaining bytes")
		return nil
	}
	out := append([]byte(nil), r.b[:n]...)
	r.b = r.b[n:]
	return out
}

func (r *canonicalReader) uvarint() uint64 {
	if r.err != nil {
		return 0
	}
	v, n := binary.Uvarint(r.b)
	if n <= 0 {
		r.fail("malformed uvarint")
		return 0
	}
	r.b = r.b[n:]
	return v
}

// count reads an element count, bounded by the bytes left so a corrupt
// count cannot force a huge allocation.
func (r *canonicalReader) count(minBytes int) uint64 {
	n := r.uvarint()
	if n > uint64(len(r.b)/minBytes) {
		r.fail("count exceeds remaining bytes")
		return 0
	}
	return n
}

func (r *canonicalReader) id() *crdtv1.OpID {
	return &crdtv1.OpID{Client: r.uvarint(), Clock: r.uvarint()}
}

func (r *canonicalReader) vector() []*crdtv1.VectorEntry {
	n := r.count(2)
	entries := make([]*crdtv1.VectorEntry, 0, n)
	for range n {
		entries = append(entries, &crdtv1.VectorEntry{Client: r.uvarint(), Clock: r.uvarint()})
	}
	return entries
}
