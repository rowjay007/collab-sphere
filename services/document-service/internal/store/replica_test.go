package store_test

import (
	"errors"
	"sync"
	"testing"
	"time"

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

	if got, _, err := st.ClaimReplica(ctx, home.ID, 77, false, "", time.Hour); err != nil || got != 77 {
		t.Fatalf("claim = %d, %v", got, err)
	}
	if got, _, err := st.ClaimReplica(ctx, home.ID, 77, false, "", time.Hour); err != nil || got != 77 {
		t.Fatalf("repeat claim = %d, %v", got, err)
	}
	if _, _, err := st.ClaimReplica(ctx, branch, 77, false, "", time.Hour); !errors.Is(err, domain.ErrReplicaHomedElsewhere) {
		t.Fatalf("branch claim error = %v", err)
	}
	if got, _, err := st.ClaimReplica(ctx, unrelated.ID, 77, false, "", time.Hour); err != nil || got != 77 {
		t.Fatalf("unrelated claim = %d, %v", got, err)
	}

	allocated, _, err := st.ClaimReplica(ctx, home.ID, 0, false, "", time.Hour)
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
			_, _, err := st.ClaimReplica(ctx, doc, 88, false, "", time.Hour)
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
	if _, _, err := st.ClaimReplica(ctx, uuid.New(), 1, false, "", time.Hour); !errors.Is(err, domain.ErrNotFound) {
		t.Fatalf("missing error = %v", err)
	}
	doc, err := st.Create(ctx, "archived", "owner")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := st.Archive(ctx, doc.ID); err != nil {
		t.Fatal(err)
	}
	if _, _, err := st.ClaimReplica(ctx, doc.ID, 1, false, "", time.Hour); !errors.Is(err, domain.ErrArchived) {
		t.Fatalf("archived error = %v", err)
	}
}

func TestClaimReplicaRefusesClientClaimOfSystemID(t *testing.T) {
	st, _ := testdb.Open(t)
	ctx := t.Context()
	doc, err := st.Create(ctx, "home", "owner")
	if err != nil {
		t.Fatal(err)
	}
	sys, _, err := st.ClaimReplica(ctx, doc.ID, 0, true, "", time.Hour)
	if err != nil || sys == 0 {
		t.Fatalf("system allocate = %d, %v", sys, err)
	}
	if _, _, err := st.ClaimReplica(ctx, doc.ID, sys, false, "", time.Hour); !errors.Is(err, domain.ErrReplicaHomedElsewhere) {
		t.Fatalf("client claim of system id = %v", err)
	}
	if got, _, err := st.ClaimReplica(ctx, doc.ID, sys, true, "", time.Hour); err != nil || got != sys {
		t.Fatalf("system re-claim = %d, %v", got, err)
	}
}

func TestReplicaUserIsWriteOnce(t *testing.T) {
	st, _ := testdb.Open(t)
	ctx := t.Context()
	doc, err := st.Create(ctx, "owned", "owner")
	if err != nil {
		t.Fatal(err)
	}
	if got, _, err := st.ClaimReplica(ctx, doc.ID, 7, false, "ada", time.Hour); err != nil || got != 7 {
		t.Fatalf("claim = %d, %v", got, err)
	}
	if _, _, err := st.ClaimReplica(ctx, doc.ID, 7, false, "ada", time.Hour); err != nil {
		t.Fatalf("reclaim = %v", err)
	}
	if _, _, err := st.ClaimReplica(ctx, doc.ID, 7, false, "bob", time.Hour); !errors.Is(err, domain.ErrReplicaOwned) {
		t.Fatalf("other user = %v", err)
	}
	if _, _, err := st.ClaimReplica(ctx, doc.ID, 8, false, "", time.Hour); err != nil {
		t.Fatal(err)
	}
	start := make(chan struct{})
	errs := make([]error, 2)
	var wg sync.WaitGroup
	for i, user := range []string{"cara", "dana"} {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			_, _, errs[i] = st.ClaimReplica(ctx, doc.ID, 8, false, user, time.Hour)
		}()
	}
	close(start)
	wg.Wait()
	var won, lost int
	for _, err := range errs {
		switch {
		case err == nil:
			won++
		case errors.Is(err, domain.ErrReplicaOwned):
			lost++
		default:
			t.Fatalf("adopt = %v", err)
		}
	}
	if won != 1 || lost != 1 {
		t.Fatalf("adopt results won=%d lost=%d errs=%v", won, lost, errs)
	}
	if _, _, err := st.ClaimReplica(ctx, doc.ID, 0, true, "ada", time.Hour); !errors.Is(err, domain.ErrInvalidArgument) {
		t.Fatalf("system with user = %v", err)
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
