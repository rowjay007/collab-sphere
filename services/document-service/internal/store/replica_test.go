package store_test

import (
	"errors"
	"sync"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/rowjay007/collab-sphere/services/document-service/internal/domain"
	"github.com/rowjay007/collab-sphere/services/document-service/internal/testdb"
)

func TestClaimReplica(t *testing.T) {
	st, pool := testdb.Open(t)
	ctx := t.Context()
	home, err := st.Create(ctx, "home", "owner")
	if err != nil {
		t.Fatal(err)
	}
	branch := insertDocumentInTree(t, pool, home.ID)
	unrelated, err := st.Create(ctx, "unrelated", "owner")
	if err != nil {
		t.Fatal(err)
	}

	if got, err := st.ClaimReplica(ctx, home.ID, 77); err != nil || got != 77 {
		t.Fatalf("claim = %d, %v", got, err)
	}
	if got, err := st.ClaimReplica(ctx, home.ID, 77); err != nil || got != 77 {
		t.Fatalf("repeat claim = %d, %v", got, err)
	}
	if _, err := st.ClaimReplica(ctx, branch, 77); !errors.Is(err, domain.ErrReplicaHomedElsewhere) {
		t.Fatalf("branch claim error = %v", err)
	}
	if got, err := st.ClaimReplica(ctx, unrelated.ID, 77); err != nil || got != 77 {
		t.Fatalf("unrelated claim = %d, %v", got, err)
	}

	allocated, err := st.ClaimReplica(ctx, home.ID, 0)
	if err != nil {
		t.Fatal(err)
	}
	if allocated == 0 || allocated == 77 {
		t.Fatalf("allocated replica = %d", allocated)
	}
}

func TestClaimReplicaConcurrentRace(t *testing.T) {
	st, pool := testdb.Open(t)
	ctx := t.Context()
	home, err := st.Create(ctx, "home", "owner")
	if err != nil {
		t.Fatal(err)
	}
	branch := insertDocumentInTree(t, pool, home.ID)

	type result struct {
		doc uuid.UUID
		err error
	}
	start := make(chan struct{})
	results := make(chan result, 2)
	var wg sync.WaitGroup
	for _, doc := range []uuid.UUID{home.ID, branch} {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			_, err := st.ClaimReplica(ctx, doc, 88)
			results <- result{doc: doc, err: err}
		}()
	}
	close(start)
	wg.Wait()
	close(results)

	var won, refused int
	for res := range results {
		switch {
		case res.err == nil:
			won++
		case errors.Is(res.err, domain.ErrReplicaHomedElsewhere):
			refused++
		default:
			t.Fatalf("claim for %s failed: %v", res.doc, res.err)
		}
	}
	if won != 1 || refused != 1 {
		t.Fatalf("won=%d refused=%d", won, refused)
	}
}

func TestClaimReplicaRejectsMissingAndArchived(t *testing.T) {
	st, _ := testdb.Open(t)
	ctx := t.Context()
	if _, err := st.ClaimReplica(ctx, uuid.New(), 1); !errors.Is(err, domain.ErrNotFound) {
		t.Fatalf("missing error = %v", err)
	}
	doc, err := st.Create(ctx, "archived", "owner")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := st.Archive(ctx, doc.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := st.ClaimReplica(ctx, doc.ID, 1); !errors.Is(err, domain.ErrArchived) {
		t.Fatalf("archived error = %v", err)
	}
}

func insertDocumentInTree(t *testing.T, pool *pgxpool.Pool, forkRoot uuid.UUID) uuid.UUID {
	t.Helper()
	id := uuid.New()
	if _, err := pool.Exec(t.Context(), `
		INSERT INTO documents (id, title, owner_id, fork_root)
		VALUES ($1, 'branch', 'owner', $2)`, id, forkRoot); err != nil {
		t.Fatal(err)
	}
	return id
}
