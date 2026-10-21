package e2e

import (
	"bytes"
	"context"
	"math/rand/v2"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"

	crdtv1 "github.com/rowjay007/collab-sphere/gen/crdt/v1"
	documentv1 "github.com/rowjay007/collab-sphere/gen/document/v1"
	versionv1 "github.com/rowjay007/collab-sphere/gen/version/v1"
	"github.com/rowjay007/collab-sphere/pkg/collabclient"
	"github.com/rowjay007/collab-sphere/pkg/crdt"
	"github.com/rowjay007/collab-sphere/tests/internal/stack"
)

func storedSeqs(t *testing.T, ctx context.Context, vs *stack.VersionService, docID string) (map[uint64][]byte, error) {
	t.Helper()
	rows, err := vs.DB().Query(ctx, `SELECT seq, id FROM nodes WHERE doc = $1`, uuid.MustParse(docID))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[uint64][]byte{}
	for rows.Next() {
		var (
			seq int64
			id  []byte
		)
		if err := rows.Scan(&seq, &id); err != nil {
			return nil, err
		}
		out[uint64(seq)] = id
	}
	return out, rows.Err()
}

func TestAutoVersions(t *testing.T) {
	s := stack.New(t, stack.WithHubs(2))
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Minute)
	defer cancel()
	swept := stack.StartVersionService(s, 200*time.Millisecond)
	touched := stack.StartVersionService(s, 0)

	docID := s.CreateDocument(ctx)
	clients := []*collabclient.Client{s.DialAllocated(ctx, docID, 0), s.DialAllocated(ctx, docID, 1)}
	var wg sync.WaitGroup
	for i, c := range clients {
		wg.Go(func() {
			rng := rand.New(rand.NewPCG(uint64(i), 99))
			for range 525 {
				if err := randomEdit(rng, c); err != nil {
					t.Error(err)
					return
				}
			}
		})
	}
	wg.Wait()
	stack.Settle(t, ctx, s.Docs, docID, clients)
	if _, meta := stack.LoadText(t, ctx, s.Docs, docID); meta.GetOpSeq() != 1050 {
		t.Fatalf("op_seq = %d, want 1050", meta.GetOpSeq())
	}

	want := []uint64{0, 100, 200, 300, 400, 500, 600, 700, 800, 900, 1000}
	var bySweep map[uint64][]byte
	for {
		var err error
		if bySweep, err = storedSeqs(t, ctx, swept, docID); err != nil {
			t.Fatal(err)
		}
		if len(bySweep) == len(want) {
			break
		}
		if len(bySweep) > len(want) {
			t.Fatalf("sweeper stored seqs %v", bySweep)
		}
		select {
		case <-ctx.Done():
			t.Fatalf("sweeper stored only %d of %d nodes", len(bySweep), len(want))
		case <-time.After(50 * time.Millisecond):
		}
	}

	versions := listVersions(t, ctx, touched.Client, docID)
	if got := versionSeqs(versions); !slices.Equal(got, want) {
		t.Fatalf("touched seqs %v, want %v", got, want)
	}
	var prev [][]byte
	for _, v := range versions {
		verifyVersion(t, ctx, s.Docs, touched.Client, v)
		if !slices.EqualFunc(v.GetParents(), prev, bytes.Equal) {
			t.Fatalf("node at %d has parents %v", v.GetSeq(), v.GetParents())
		}
		if !bytes.Equal(bySweep[v.GetSeq()], v.GetId()) {
			t.Fatalf("node at %d: sweeper stored %s, touch stored %s", v.GetSeq(), shortID(bySweep[v.GetSeq()]), shortID(v.GetId()))
		}
		wantLabel := "auto"
		if v.GetSeq() == 0 {
			wantLabel = "genesis"
		}
		if !slices.Equal(v.GetLabels(), []string{wantLabel}) {
			t.Fatalf("node at %d labels %v", v.GetSeq(), v.GetLabels())
		}
		prev = [][]byte{v.GetId()}
	}
}

func appendRandomOps(t *testing.T, ctx context.Context, docs documentv1.DocumentServiceClient, docID string, n int) {
	t.Helper()
	r, err := crdt.NewReplica(7, nil)
	if err != nil {
		t.Fatal(err)
	}
	rng := rand.New(rand.NewPCG(7, 7))
	for n > 0 {
		var batch []*crdtv1.Operation
		for range min(n, 1000) {
			var op crdt.Op
			if r.Len() > 0 && rng.IntN(4) == 0 {
				op, err = r.DeleteAt(rng.IntN(r.Len()))
			} else {
				op, err = r.InsertAt(rng.IntN(r.Len()+1), rune(letters[rng.IntN(len(letters))]))
			}
			if err != nil {
				t.Fatal(err)
			}
			batch = append(batch, crdt.OpToProto(op))
		}
		if _, err := docs.AppendOperations(ctx, &documentv1.AppendOperationsRequest{Id: docID, Operations: batch}); err != nil {
			t.Fatal(err)
		}
		n -= len(batch)
	}
}

func TestVersionServiceKilledMidMaterializationConverges(t *testing.T) {
	s := stack.New(t)
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Minute)
	defer cancel()
	const ops, nodes = 10_000, 101
	docID := s.CreateDocument(ctx)
	appendRandomOps(t, ctx, s.Docs, docID, ops)

	vs := stack.StartVersionService(s, 0)
	go vs.Client.ListVersions(ctx, &versionv1.ListVersionsRequest{DocId: docID})
	for {
		stored, err := storedSeqs(t, ctx, vs, docID)
		if err != nil {
			t.Fatal(err)
		}
		if len(stored) >= 3 {
			break
		}
		time.Sleep(time.Millisecond)
	}
	vs.Kill()
	stored, err := storedSeqs(t, ctx, vs, docID)
	if err != nil {
		t.Fatal(err)
	}
	if len(stored) >= nodes {
		t.Fatalf("materialization finished before the kill (%d nodes); the run proves nothing", len(stored))
	}
	t.Logf("killed with %d of %d nodes stored", len(stored), nodes)

	vs.Start()
	resumed := listVersions(t, ctx, vs.Client, docID)
	reference := listVersions(t, ctx, stack.StartVersionService(s, 0).Client, docID)
	if len(resumed) != nodes || len(reference) != nodes {
		t.Fatalf("resumed %d nodes, reference %d, want %d", len(resumed), len(reference), nodes)
	}
	for i, v := range resumed {
		if v.GetSeq() != uint64(i*100) || !bytes.Equal(v.GetId(), reference[i].GetId()) {
			t.Fatalf("node %d: resumed %d %s, reference %d %s", i, v.GetSeq(), shortID(v.GetId()), reference[i].GetSeq(), shortID(reference[i].GetId()))
		}
		if id, ok := stored[v.GetSeq()]; ok && !bytes.Equal(id, v.GetId()) {
			t.Fatalf("node at %d changed id across the restart", v.GetSeq())
		}
		if !bytes.Equal(recomputeID(t, v), v.GetId()) {
			t.Fatalf("node at %d id does not recompute", v.GetSeq())
		}
	}
	verifyVersion(t, ctx, s.Docs, vs.Client, resumed[len(resumed)-1])
}
