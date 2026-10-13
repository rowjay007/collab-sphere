package store_test

import (
	"errors"
	"testing"

	"github.com/google/uuid"

	crdtv1 "github.com/rowjay007/collab-sphere/gen/crdt/v1"
	"github.com/rowjay007/collab-sphere/pkg/crdt"
	"github.com/rowjay007/collab-sphere/services/document-service/internal/domain"
	"github.com/rowjay007/collab-sphere/services/document-service/internal/store"
	"github.com/rowjay007/collab-sphere/services/document-service/internal/testdb"
)

func TestCreateGetArchive(t *testing.T) {
	st, _ := testdb.Open(t)
	ctx := t.Context()

	doc, err := st.Create(ctx, "notes", "owner-1")
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if doc.Title != "notes" || doc.OwnerID != "owner-1" || doc.Status != domain.StatusActive {
		t.Fatalf("created %+v", doc)
	}
	if doc.OpSeq != 0 || doc.SnapshotSeq != 0 {
		t.Fatalf("seqs %+v", doc)
	}

	got, err := st.Get(ctx, doc.ID)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if got.ID != doc.ID || got.Title != "notes" {
		t.Fatalf("get %+v", got)
	}

	if _, err := st.Get(ctx, uuid.New()); !errors.Is(err, domain.ErrNotFound) {
		t.Fatalf("unknown get: %v", err)
	}

	archived, err := st.Archive(ctx, doc.ID)
	if err != nil {
		t.Fatalf("archive: %v", err)
	}
	if archived.Status != domain.StatusArchived {
		t.Fatalf("status %s", archived.Status)
	}
	again, err := st.Archive(ctx, doc.ID)
	if err != nil {
		t.Fatalf("archive again: %v", err)
	}
	if again.Status != domain.StatusArchived {
		t.Fatalf("idempotent archive %s", again.Status)
	}

	op, err := insertProto(t, 1, 0, 'a')
	if err != nil {
		t.Fatal(err)
	}
	if _, err := st.Append(ctx, doc.ID, []*crdtv1.Operation{op}); !errors.Is(err, domain.ErrArchived) {
		t.Fatalf("append archived: %v", err)
	}
}

func TestCreateRequiresFields(t *testing.T) {
	st, _ := testdb.Open(t)
	ctx := t.Context()
	if _, err := st.Create(ctx, "", "owner"); !errors.Is(err, domain.ErrInvalidArgument) {
		t.Fatalf("empty title: %v", err)
	}
	if _, err := st.Create(ctx, "title", ""); !errors.Is(err, domain.ErrInvalidArgument) {
		t.Fatalf("empty owner: %v", err)
	}
}

func TestMigrateIdempotent(t *testing.T) {
	_, pool := testdb.Open(t)
	if err := store.Migrate(t.Context(), pool); err != nil {
		t.Fatalf("second migrate: %v", err)
	}
}

func insertProto(t *testing.T, client crdt.ClientID, index int, value rune) (*crdtv1.Operation, error) {
	t.Helper()
	r, err := crdt.NewReplica(client, nil)
	if err != nil {
		return nil, err
	}
	op, err := r.InsertAt(index, value)
	if err != nil {
		return nil, err
	}
	return testdb.ProtoFromInsert(op), nil
}
