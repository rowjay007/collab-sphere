package store_test

import (
	"bytes"
	"strings"
	"testing"

	"google.golang.org/protobuf/encoding/protojson"

	crdtv1 "github.com/rowjay007/collab-sphere/gen/crdt/v1"
	"github.com/rowjay007/collab-sphere/pkg/crdt"
	"github.com/rowjay007/collab-sphere/services/document-service/internal/domain"
	"github.com/rowjay007/collab-sphere/services/document-service/internal/testdb"
)

func TestLiveSnapshotUpsertOneRowPerDocument(t *testing.T) {
	st, pool := testdb.Open(t)
	ctx := t.Context()
	doc, err := st.Create(ctx, "live", "owner")
	if err != nil {
		t.Fatal(err)
	}
	r, err := crdt.NewReplica(1, nil)
	if err != nil {
		t.Fatal(err)
	}
	op, err := r.InsertAfter(crdt.Root, 'x')
	if err != nil {
		t.Fatal(err)
	}
	if _, err := st.Append(ctx, doc.ID, []*crdtv1.Operation{crdt.OpToProto(op)}); err != nil {
		t.Fatal(err)
	}
	d, _, err := st.BuildDocThrough(ctx, doc.ID, 1)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := d.GCWithPins(d.Vector(), nil); err != nil {
		t.Fatal(err)
	}
	if err := st.UpsertLiveSnapshot(ctx, doc.ID, 1, d.Snapshot()); err != nil {
		t.Fatal(err)
	}
	if err := st.UpsertLiveSnapshot(ctx, doc.ID, 1, d.Snapshot()); err != nil {
		t.Fatal(err)
	}
	var count int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM snapshots WHERE document_id = $1 AND kind = 'live'`, doc.ID).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatalf("live rows=%d", count)
	}
}

func TestLoadStatePrefersLiveSnapshot(t *testing.T) {
	st, _ := testdb.Open(t)
	ctx := t.Context()
	doc, err := st.Create(ctx, "live-load", "owner")
	if err != nil {
		t.Fatal(err)
	}
	r, err := crdt.NewReplica(1, nil)
	if err != nil {
		t.Fatal(err)
	}
	var ops []*crdtv1.Operation
	for _, ch := range []rune("abc") {
		op, err := r.InsertAfter(crdt.Root, ch)
		if err != nil {
			t.Fatal(err)
		}
		ops = append(ops, crdt.OpToProto(op))
	}
	if _, err := st.Append(ctx, doc.ID, ops); err != nil {
		t.Fatal(err)
	}
	full, _, err := st.BuildDocThrough(ctx, doc.ID, 3)
	if err != nil {
		t.Fatal(err)
	}
	want := mustText(t, full)
	liveDoc, _, err := st.BuildDocThrough(ctx, doc.ID, 2)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := liveDoc.GCWithPins(liveDoc.Vector(), nil); err != nil {
		t.Fatal(err)
	}
	if err := st.UpsertLiveSnapshot(ctx, doc.ID, 2, liveDoc.Snapshot()); err != nil {
		t.Fatal(err)
	}
	_, snap, tail, err := st.LoadState(ctx, doc.ID)
	if err != nil {
		t.Fatal(err)
	}
	got, err := crdt.FromSnapshot(snap)
	if err != nil {
		t.Fatal(err)
	}
	for _, pb := range tail {
		op, err := crdt.OpFromProto(pb)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := got.Apply(op); err != nil {
			t.Fatal(err)
		}
	}
	if mustText(t, got) != want {
		t.Fatalf("text %q != %q", mustText(t, got), want)
	}
}

func TestEncryptedLiveSnapshotHasNoRune(t *testing.T) {
	st, pool := testdb.Open(t)
	ctx := t.Context()
	doc, err := st.CreateWithEncryption(ctx, "enc", "owner", true)
	if err != nil {
		t.Fatal(err)
	}
	cipher := bytes.Repeat([]byte{0x11}, 17)
	insert := crdt.OpToProto(crdt.Insert{
		ID: crdt.ID{Client: 1, Clock: 1}, Parent: crdt.Root,
		Cipher: &crdt.Ciphertext{Epoch: 1, Bytes: cipher},
	})
	if _, err := st.SetEpoch(ctx, doc.ID, 1, domain.EpochOpen); err != nil {
		t.Fatal(err)
	}
	if _, err := st.Append(ctx, doc.ID, []*crdtv1.Operation{insert}); err != nil {
		t.Fatal(err)
	}
	d, _, err := st.BuildDocThrough(ctx, doc.ID, 1)
	if err != nil {
		t.Fatal(err)
	}
	if err := st.UpsertLiveSnapshot(ctx, doc.ID, 1, d.Snapshot()); err != nil {
		t.Fatal(err)
	}
	var raw []byte
	if err := pool.QueryRow(ctx, `SELECT state FROM snapshots WHERE document_id = $1 AND kind = 'live'`, doc.ID).Scan(&raw); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(raw), "z") {
		t.Fatal("live snapshot JSON contains plaintext marker")
	}
	snap := &crdtv1.Snapshot{}
	if err := protojson.Unmarshal(raw, snap); err != nil {
		t.Fatal(err)
	}
	for _, n := range snap.GetNodes() {
		if n.GetValue() != 0 {
			t.Fatalf("node value=%d", n.GetValue())
		}
	}
}
