package e2e

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"testing"

	"github.com/google/uuid"

	documentv1 "github.com/rowjay007/collab-sphere/gen/document/v1"
	versionv1 "github.com/rowjay007/collab-sphere/gen/version/v1"
	"github.com/rowjay007/collab-sphere/pkg/crdt"
	"github.com/rowjay007/collab-sphere/tests/internal/oracle"
)

// recomputeID writes design §2's node preimage from the version's stored
// fields, independently of version-service's dag package.
func recomputeID(t *testing.T, v *versionv1.Version) []byte {
	t.Helper()
	doc, err := uuid.Parse(v.GetDocId())
	if err != nil {
		t.Fatal(err)
	}
	b := []byte("CSV1")
	b = append(b, doc[:]...)
	b = binary.AppendUvarint(b, v.GetSeq())
	b = append(b, v.GetContentHash()...)
	b = binary.AppendUvarint(b, uint64(len(v.GetParents())))
	for _, p := range v.GetParents() {
		b = append(b, p...)
	}
	sum := sha256.Sum256(b)
	return sum[:]
}

func readOps(t *testing.T, ctx context.Context, docs documentv1.DocumentServiceClient, docID string, upTo uint64) []crdt.Op {
	t.Helper()
	var ops []crdt.Op
	for after := uint64(0); after < upTo; {
		resp, err := docs.ReadOperations(ctx, &documentv1.ReadOperationsRequest{Id: docID, AfterSeq: after, Limit: uint32(min(upTo-after, 10_000))})
		if err != nil {
			t.Fatal(err)
		}
		if len(resp.GetOperations()) == 0 {
			t.Fatalf("log of %s ends at %d, before %d", docID, after, upTo)
		}
		for _, p := range resp.GetOperations() {
			op, err := crdt.OpFromProto(p)
			if err != nil {
				t.Fatal(err)
			}
			ops = append(ops, op)
		}
		after += uint64(len(resp.GetOperations()))
	}
	return ops
}

func oracleState(t *testing.T, ctx context.Context, docs documentv1.DocumentServiceClient, docID string, seq uint64) oracle.State {
	t.Helper()
	st, err := oracle.Replay(readOps(t, ctx, docs, docID, seq))
	if err != nil {
		t.Fatal(err)
	}
	return st
}

// verifyVersion checks a stored version three ways: the independent encoder
// over a replay of its log prefix gives its content hash, the hash of its
// blob matches, and its ID recomputes from its fields.
func verifyVersion(t *testing.T, ctx context.Context, docs documentv1.DocumentServiceClient, vs versionv1.VersionServiceClient, v *versionv1.Version) {
	t.Helper()
	want := sha256.Sum256(oracle.Encode(oracleState(t, ctx, docs, v.GetDocId(), v.GetSeq())))
	if !bytes.Equal(v.GetContentHash(), want[:]) {
		t.Fatalf("%s@%d: content hash %x, oracle %x", v.GetDocId(), v.GetSeq(), v.GetContentHash(), want)
	}
	blob, err := vs.GetBlob(ctx, &versionv1.GetBlobRequest{Hash: v.GetContentHash()})
	if err != nil {
		t.Fatal(err)
	}
	if got := sha256.Sum256(blob.GetBytes()); !bytes.Equal(got[:], want[:]) {
		t.Fatalf("%s@%d: blob hashes to %x", v.GetDocId(), v.GetSeq(), got)
	}
	if id := recomputeID(t, v); !bytes.Equal(id, v.GetId()) {
		t.Fatalf("%s@%d: id %x, recomputed %x", v.GetDocId(), v.GetSeq(), v.GetId(), id)
	}
}

func listVersions(t *testing.T, ctx context.Context, vs versionv1.VersionServiceClient, docID string) []*versionv1.Version {
	t.Helper()
	var out []*versionv1.Version
	token := ""
	for {
		resp, err := vs.ListVersions(ctx, &versionv1.ListVersionsRequest{DocId: docID, PageSize: 50, PageToken: token})
		if err != nil {
			t.Fatal(err)
		}
		out = append(out, resp.GetVersions()...)
		if token = resp.GetNextPageToken(); token == "" {
			return out
		}
	}
}

func versionSeqs(vs []*versionv1.Version) []uint64 {
	out := make([]uint64, len(vs))
	for i, v := range vs {
		out[i] = v.GetSeq()
	}
	return out
}

func shortID(id []byte) string {
	return hex.EncodeToString(id)[:12]
}
