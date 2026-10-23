package store

import (
	"context"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/rowjay007/collab-sphere/services/version-service/internal/dag"
)

const (
	KindMerge   = "merge"
	KindRestore = "restore"

	StatePlanned  = "planned"
	StateAppended = "appended"
	StateDone     = "done"
	StateFailed   = "failed"
)

type Mutation struct {
	RequestID  string
	Doc        uuid.UUID
	Kind       string
	State      string
	SourceDoc  *uuid.UUID
	SourceNode *dag.NodeID
	TargetNode *dag.NodeID
	Plan       []byte
	ResultNode *dag.NodeID
	Appended   int
	Noop       bool
}

func (s *Store) Mutation(ctx context.Context, requestID string) (Mutation, bool, error) {
	return optionalMutation(scanMutation(s.pool.QueryRow(ctx, selectMutation+` WHERE request_id = $1`, requestID)))
}

func (t Tx) Mutation(ctx context.Context, requestID string) (Mutation, bool, error) {
	return optionalMutation(scanMutation(t.tx.QueryRow(ctx, selectMutation+` WHERE request_id = $1`, requestID)))
}

func (t Tx) Pending(ctx context.Context, docs []uuid.UUID) ([]Mutation, error) {
	rows, err := t.tx.Query(ctx, selectMutation+` WHERE doc = ANY($1) AND state IN ('planned', 'appended')`, docs)
	if err != nil {
		return nil, fmt.Errorf("list pending mutations: %w", err)
	}
	defer rows.Close()
	var out []Mutation
	for rows.Next() {
		m, err := scanMutation(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, m)
	}
	return out, rows.Err()
}

func (t Tx) InsertMutation(ctx context.Context, m Mutation) error {
	var sourceDoc any
	if m.SourceDoc != nil {
		sourceDoc = *m.SourceDoc
	}
	_, err := t.tx.Exec(ctx, `
		INSERT INTO mutations (request_id, doc, kind, state, source_doc, source_node, target_node, plan, result_node, appended, noop)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)`,
		m.RequestID, m.Doc, m.Kind, m.State, sourceDoc, nodeBytes(m.SourceNode), nodeBytes(m.TargetNode),
		m.Plan, nodeBytes(m.ResultNode), m.Appended, m.Noop)
	if err != nil {
		return fmt.Errorf("insert mutation: %w", err)
	}
	return nil
}

func (t Tx) SetMutationState(ctx context.Context, requestID, state string) error {
	if _, err := t.tx.Exec(ctx, `UPDATE mutations SET state = $2 WHERE request_id = $1`, requestID, state); err != nil {
		return fmt.Errorf("set mutation state: %w", err)
	}
	return nil
}

func (t Tx) FinishMutation(ctx context.Context, requestID string, result dag.NodeID, appended int, noop bool) error {
	if _, err := t.tx.Exec(ctx, `
		UPDATE mutations SET state = $2, result_node = $3, appended = $4, noop = $5
		WHERE request_id = $1`, requestID, StateDone, result[:], appended, noop); err != nil {
		return fmt.Errorf("finish mutation: %w", err)
	}
	return nil
}

func (t Tx) SystemReplica(ctx context.Context, doc uuid.UUID) (uint64, bool, error) {
	var replica *int64
	if err := t.tx.QueryRow(ctx, `SELECT system_replica FROM version_docs WHERE doc = $1`, doc).Scan(&replica); err != nil {
		if err == pgx.ErrNoRows {
			return 0, false, nil
		}
		return 0, false, fmt.Errorf("system replica: %w", err)
	}
	if replica == nil {
		return 0, false, nil
	}
	return uint64(*replica), true, nil
}

func (t Tx) SetSystemReplica(ctx context.Context, doc uuid.UUID, replica uint64) error {
	if _, err := t.tx.Exec(ctx, `
		UPDATE version_docs SET system_replica = $2
		WHERE doc = $1 AND system_replica IS NULL`, doc, int64(replica)); err != nil {
		return fmt.Errorf("set system replica: %w", err)
	}
	return nil
}

const selectMutation = `
	SELECT request_id, doc, kind, state, source_doc, source_node, target_node, plan, result_node, appended, noop
	FROM mutations`

func scanMutation(row pgx.Row) (Mutation, error) {
	var (
		m                           Mutation
		sourceDoc                   *uuid.UUID
		sourceNode, targetNode, res []byte
	)
	if err := row.Scan(&m.RequestID, &m.Doc, &m.Kind, &m.State, &sourceDoc, &sourceNode, &targetNode, &m.Plan, &res, &m.Appended, &m.Noop); err != nil {
		return Mutation{}, err
	}
	m.SourceDoc = sourceDoc
	m.SourceNode = nodeIDPtr(sourceNode)
	m.TargetNode = nodeIDPtr(targetNode)
	m.ResultNode = nodeIDPtr(res)
	return m, nil
}

func optionalMutation(m Mutation, err error) (Mutation, bool, error) {
	if err == pgx.ErrNoRows {
		return Mutation{}, false, nil
	}
	if err != nil {
		return Mutation{}, false, err
	}
	return m, true, nil
}

func nodeBytes(id *dag.NodeID) any {
	if id == nil {
		return nil
	}
	return id[:]
}

func nodeIDPtr(b []byte) *dag.NodeID {
	if len(b) != len(dag.NodeID{}) {
		return nil
	}
	id := dag.NodeID(b)
	return &id
}
