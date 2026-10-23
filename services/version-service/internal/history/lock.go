package history

import (
	"context"
	"errors"
	"fmt"

	"github.com/google/uuid"

	"github.com/rowjay007/collab-sphere/services/version-service/internal/store"
)

type heldKey struct{}

var errPending = errors.New("history: pending mutation")

func holdFrom(ctx context.Context) *store.Hold {
	h, _ := ctx.Value(heldKey{}).(*store.Hold)
	return h
}

func (h *History) ensureHold(ctx context.Context, docs []uuid.UUID) (context.Context, func(), error) {
	if hold := holdFrom(ctx); hold != nil {
		if hold.Covers(docs) {
			return ctx, func() {}, nil
		}
		return ctx, nil, fmt.Errorf("hold does not cover %v", docs)
	}
	hold, err := h.store.Hold(ctx, docs)
	if err != nil {
		return ctx, nil, err
	}
	return context.WithValue(ctx, heldKey{}, hold), hold.Release, nil
}

func (h *History) do(ctx context.Context, docs []uuid.UUID, fn func(store.Tx) error) error {
	if hold := holdFrom(ctx); hold != nil {
		return hold.Do(ctx, docs, fn)
	}
	ctx, unlock, err := h.ensureHold(ctx, docs)
	if err != nil {
		return err
	}
	defer unlock()
	return holdFrom(ctx).Do(ctx, docs, fn)
}

// withDoc holds the documents, drives each pending mutation to done, then
// runs fn in a short transaction. document-service RPCs run outside that
// transaction, under the same advisory locks.
func (h *History) withDoc(ctx context.Context, docs []uuid.UUID, fn func(store.Tx) error) error {
	ctx, unlock, err := h.ensureHold(ctx, docs)
	if err != nil {
		return err
	}
	defer unlock()
	for {
		var pending []store.Mutation
		err := holdFrom(ctx).Do(ctx, docs, func(tx store.Tx) error {
			var err error
			pending, err = tx.Pending(ctx, docs)
			if err != nil {
				return err
			}
			if len(pending) > 0 {
				return errPending
			}
			return fn(tx)
		})
		if err == nil {
			return nil
		}
		if !errors.Is(err, errPending) {
			return err
		}
		for _, m := range pending {
			if err := h.drive(ctx, m); err != nil {
				return err
			}
		}
	}
}
