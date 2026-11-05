package perms

import (
	"context"
	"errors"
	"time"

	"github.com/google/uuid"
)

type epochRow struct {
	Doc   uuid.UUID
	Epoch uint32
	State string
}

func (s *Service) pushCurrent(ctx context.Context, doc uuid.UUID) error {
	if s.epochs == nil {
		return errors.New("permission: epoch client is not configured")
	}
	epoch, state, ok, err := s.db.currentEpoch(ctx, doc)
	if err != nil || !ok {
		return err
	}
	if err := s.epochs.SetEpoch(ctx, doc, epoch, state); err != nil {
		return err
	}
	return s.db.markPushed(ctx, doc, epoch, state)
}

func (s *Service) RunEpochSync(ctx context.Context) {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.reconcileEpochs(ctx)
		}
	}
}

func (s *Service) reconcileEpochs(ctx context.Context) {
	rows, err := s.db.unpushed(ctx)
	if err != nil {
		return
	}
	for _, row := range rows {
		_ = s.pushCurrent(ctx, row.Doc)
	}
}

func (s *store) markPushed(ctx context.Context, doc uuid.UUID, epoch uint32, state string) error {
	_, err := s.pool.Exec(ctx, `
		UPDATE doc_epochs SET pushed = true
		WHERE doc_id = $1 AND epoch = $2 AND state = $3 AND NOT pushed`, doc, int(epoch), state)
	return err
}

func (s *store) unpushed(ctx context.Context) ([]epochRow, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT e.doc_id, e.epoch, e.state
		FROM doc_epochs e
		WHERE NOT e.pushed
		  AND e.epoch = (SELECT MAX(epoch) FROM doc_epochs WHERE doc_id = e.doc_id)`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []epochRow
	for rows.Next() {
		var row epochRow
		var epoch int32
		if err := rows.Scan(&row.Doc, &epoch, &row.State); err != nil {
			return nil, err
		}
		row.Epoch = uint32(epoch)
		out = append(out, row)
	}
	return out, rows.Err()
}
