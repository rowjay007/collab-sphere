package oracle

// Encode writes the canonical snapshot bytes of ADR 0010.
func Encode(s State) []byte {
	b := []byte("CSS1")
	b = leb128(b, uint64(len(s.Nodes)))
	for _, n := range s.Nodes {
		b = leb128(b, uint64(n.ID.Client))
		b = leb128(b, n.ID.Clock)
		b = leb128(b, uint64(n.Parent.Client))
		b = leb128(b, n.Parent.Clock)
		b = leb128(b, uint64(n.Value))
		var flags uint64
		if n.Copy {
			flags = 1
		}
		b = leb128(b, flags)
		b = leb128(b, uint64(len(n.DeletedBy)))
		for _, d := range n.DeletedBy {
			b = leb128(b, uint64(d.Client))
			b = leb128(b, d.Clock)
		}
	}
	for _, vector := range [][]Entry{s.Vector, s.Collected} {
		b = leb128(b, uint64(len(vector)))
		for _, e := range vector {
			b = leb128(b, uint64(e.Client))
			b = leb128(b, e.Clock)
		}
	}
	return b
}

func leb128(b []byte, v uint64) []byte {
	for v >= 0x80 {
		b = append(b, byte(v)|0x80)
		v >>= 7
	}
	return append(b, byte(v))
}
