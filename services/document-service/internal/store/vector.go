package store

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/rowjay007/collab-sphere/pkg/crdt"
	"github.com/rowjay007/collab-sphere/services/document-service/internal/domain"
)

func marshalVector(v crdt.VersionVector) ([]byte, error) {
	obj := make(map[string]uint64, len(v))
	for client, clock := range v {
		if client == 0 || clock == 0 {
			continue
		}
		obj[strconv.FormatUint(uint64(client), 10)] = clock
	}
	body, err := json.Marshal(obj)
	if err != nil {
		return nil, fmt.Errorf("encode vector: %w", err)
	}
	return body, nil
}

func unmarshalVector(raw []byte) (crdt.VersionVector, error) {
	if len(raw) == 0 || bytes.Equal(raw, []byte("null")) {
		return crdt.VersionVector{}, nil
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	var obj map[string]json.Number
	if err := dec.Decode(&obj); err != nil {
		return nil, fmt.Errorf("%w: vector", domain.ErrInvalidArgument)
	}
	out := make(crdt.VersionVector, len(obj))
	for key, num := range obj {
		client, err := strconv.ParseUint(key, 10, 64)
		if err != nil || client == 0 {
			return nil, fmt.Errorf("%w: vector client", domain.ErrInvalidArgument)
		}
		clock, err := strconv.ParseUint(num.String(), 10, 64)
		if err != nil || clock == 0 {
			return nil, fmt.Errorf("%w: vector clock", domain.ErrInvalidArgument)
		}
		out[crdt.ClientID(client)] = clock
	}
	return out, nil
}

// vectorBelow reports whether incoming is strictly behind stored on any client.
// A client present only in stored counts as clock 0 on incoming.
func vectorBelow(stored, incoming crdt.VersionVector) bool {
	for client, clock := range stored {
		if clock == 0 {
			continue
		}
		if incoming[client] < clock {
			return true
		}
	}
	return false
}

func advanceVector(v crdt.VersionVector, ops []pendingOp) {
	for _, op := range ops {
		if op.skip || op.key.client == 0 || op.key.clock == 0 {
			continue
		}
		client := crdt.ClientID(op.key.client)
		if op.key.clock > v[client] {
			v[client] = op.key.clock
		}
	}
}

func anyNewOp(ops []pendingOp) bool {
	for _, op := range ops {
		if !op.skip {
			return true
		}
	}
	return false
}

func stampVector(ctx context.Context, tx pgx.Tx, id uuid.UUID, snapshotSeq uint64, pending []pendingOp) error {
	var raw []byte
	if err := tx.QueryRow(ctx, `SELECT vector FROM documents WHERE id = $1`, id).Scan(&raw); err != nil {
		return fmt.Errorf("load document vector: %w", err)
	}
	if len(raw) > 0 && !anyNewOp(pending) {
		return nil
	}
	var (
		vector crdt.VersionVector
		err    error
	)
	if len(raw) == 0 {
		vector, err = replayVector(ctx, tx, id, snapshotSeq)
	} else {
		vector, err = unmarshalVector(raw)
		if err == nil {
			advanceVector(vector, pending)
		}
	}
	if err != nil {
		return err
	}
	return saveVector(ctx, tx, id, vector)
}

func loadDocumentVector(ctx context.Context, tx pgx.Tx, id uuid.UUID, persist bool) (crdt.VersionVector, uint64, error) {
	q := `SELECT op_seq, snapshot_seq, vector FROM documents WHERE id = $1`
	if persist {
		q += ` FOR UPDATE`
	}
	var opSeq, snapSeq int64
	var raw []byte
	err := tx.QueryRow(ctx, q, id).Scan(&opSeq, &snapSeq, &raw)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, 0, domain.ErrNotFound
	}
	if err != nil {
		return nil, 0, fmt.Errorf("load document vector: %w", err)
	}
	if len(raw) > 0 {
		vector, err := unmarshalVector(raw)
		return vector, uint64(opSeq), err
	}
	vector, err := replayVector(ctx, tx, id, uint64(snapSeq))
	if err != nil {
		return nil, 0, err
	}
	if persist {
		if err := saveVector(ctx, tx, id, vector); err != nil {
			return nil, 0, err
		}
	}
	return vector, uint64(opSeq), nil
}

func saveVector(ctx context.Context, tx pgx.Tx, id uuid.UUID, vector crdt.VersionVector) error {
	body, err := marshalVector(vector)
	if err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `UPDATE documents SET vector = $2::jsonb WHERE id = $1`, id, body); err != nil {
		return fmt.Errorf("store document vector: %w", err)
	}
	return nil
}

func replayVector(ctx context.Context, tx pgx.Tx, id uuid.UUID, snapshotSeq uint64) (crdt.VersionVector, error) {
	snap, err := loadSnapshot(ctx, tx, id, snapshotSeq)
	if err != nil {
		return nil, err
	}
	doc, err := crdt.FromSnapshot(snap)
	if err != nil {
		return nil, fmt.Errorf("restore snapshot: %w", err)
	}
	ops, err := loadOpsAfter(ctx, tx, id, snapshotSeq)
	if err != nil {
		return nil, err
	}
	for _, pb := range ops {
		op, err := crdt.OpFromProto(pb)
		if err != nil {
			return nil, fmt.Errorf("%w: %v", domain.ErrInvalidArgument, err)
		}
		if _, err := doc.Apply(op); err != nil {
			return nil, fmt.Errorf("replay vector: %w", err)
		}
	}
	return doc.Vector(), nil
}
