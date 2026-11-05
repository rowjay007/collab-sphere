package perms_test

import (
	"bytes"
	"context"
	"errors"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/rowjay007/collab-sphere/services/permission-service/internal/perms"
	"github.com/rowjay007/collab-sphere/services/permission-service/internal/perms/testdb"
)

func TestEnsureOwner(t *testing.T) {
	svc, ctx := setup(t)
	doc := uuid.New()
	g, err := svc.EnsureOwner(ctx, doc, "alice")
	if err != nil || g.Role != perms.RoleOwner || g.UserID != "alice" {
		t.Fatalf("first: %+v %v", g, err)
	}
	again, err := svc.EnsureOwner(ctx, doc, "alice")
	if err != nil || again.UserID != "alice" {
		t.Fatalf("idempotent: %+v %v", again, err)
	}
	if _, err := svc.EnsureOwner(ctx, doc, "bob"); !errors.Is(err, perms.ErrAlreadyExists) {
		t.Fatalf("second owner: %v", err)
	}
	if _, err := svc.EnsureOwner(ctx, doc, "Bad"); !errors.Is(err, perms.ErrInvalidArgument) {
		t.Fatalf("handle: %v", err)
	}
}

func TestGrantRevokeRules(t *testing.T) {
	svc, ctx := setup(t)
	doc := uuid.New()
	if _, err := svc.EnsureOwner(ctx, doc, "alice"); err != nil {
		t.Fatal(err)
	}
	if _, err := svc.Grant(ctx, "bob", doc, "carol", perms.RoleEditor); !errors.Is(err, perms.ErrPermissionDenied) {
		t.Fatalf("non-owner grant: %v", err)
	}
	g, err := svc.Grant(ctx, "alice", doc, "bob", perms.RoleEditor)
	if err != nil || g.Role != perms.RoleEditor {
		t.Fatalf("grant: %+v %v", g, err)
	}
	if _, err := svc.Grant(ctx, "alice", doc, "carol", perms.RoleOwner); !errors.Is(err, perms.ErrInvalidArgument) {
		t.Fatalf("grant owner: %v", err)
	}
	if _, err := svc.Grant(ctx, "alice", doc, "alice", perms.RoleEditor); !errors.Is(err, perms.ErrAlreadyExists) {
		t.Fatalf("change owner: %v", err)
	}
	if err := svc.Revoke(ctx, "alice", doc, "alice"); !errors.Is(err, perms.ErrOwnerNotRevocable) {
		t.Fatalf("revoke owner: %v", err)
	}
	if err := svc.Revoke(ctx, "bob", doc, "bob"); !errors.Is(err, perms.ErrPermissionDenied) {
		t.Fatalf("editor revoke: %v", err)
	}
	if err := svc.Revoke(ctx, "alice", doc, "bob"); err != nil {
		t.Fatal(err)
	}
	if _, err := svc.Lookup(ctx, doc, "bob"); !errors.Is(err, perms.ErrNotFound) {
		t.Fatalf("after revoke: %v", err)
	}
}

func TestCreateLinkRules(t *testing.T) {
	svc, ctx := setup(t)
	doc := uuid.New()
	if _, err := svc.EnsureOwner(ctx, doc, "alice"); err != nil {
		t.Fatal(err)
	}
	if _, err := svc.Grant(ctx, "alice", doc, "bob", perms.RoleEditor); err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 11, 5, 12, 0, 0, 0, time.UTC)
	svc.SetNow(func() time.Time { return now })

	if _, _, err := svc.CreateLink(ctx, "bob", doc, perms.RoleViewer, now.Add(time.Hour)); !errors.Is(err, perms.ErrPermissionDenied) {
		t.Fatalf("editor link: %v", err)
	}
	if _, _, err := svc.CreateLink(ctx, "alice", doc, perms.RoleOwner, now.Add(time.Hour)); !errors.Is(err, perms.ErrInvalidArgument) {
		t.Fatalf("owner link: %v", err)
	}
	if _, _, err := svc.CreateLink(ctx, "alice", doc, perms.RoleViewer, now.Add(30*time.Second)); !errors.Is(err, perms.ErrInvalidArgument) {
		t.Fatalf("short ttl: %v", err)
	}
	if _, _, err := svc.CreateLink(ctx, "alice", doc, perms.RoleViewer, now.Add(31*24*time.Hour)); !errors.Is(err, perms.ErrInvalidArgument) {
		t.Fatalf("long ttl: %v", err)
	}
	l, tok, err := svc.CreateLink(ctx, "alice", doc, perms.RoleCommenter, now.Add(time.Hour))
	if err != nil || tok == "" || l.Role != perms.RoleCommenter {
		t.Fatalf("create: %+v %q %v", l, tok, err)
	}
}

func TestRedeemNeverDemotes(t *testing.T) {
	svc, ctx := setup(t)
	doc := uuid.New()
	if _, err := svc.EnsureOwner(ctx, doc, "alice"); err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 11, 5, 12, 0, 0, 0, time.UTC)
	svc.SetNow(func() time.Time { return now })
	_, tok, err := svc.CreateLink(ctx, "alice", doc, perms.RoleViewer, now.Add(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	g, err := svc.Redeem(ctx, "alice", tok)
	if err != nil || g.Role != perms.RoleOwner {
		t.Fatalf("owner redeem: %+v %v", g, err)
	}
	if _, err := svc.Grant(ctx, "alice", doc, "bob", perms.RoleEditor); err != nil {
		t.Fatal(err)
	}
	g, err = svc.Redeem(ctx, "bob", tok)
	if err != nil || g.Role != perms.RoleEditor {
		t.Fatalf("editor redeem viewer: %+v %v", g, err)
	}
	g, err = svc.Redeem(ctx, "carol", tok)
	if err != nil || g.Role != perms.RoleViewer {
		t.Fatalf("new redeem: %+v %v", g, err)
	}
}

func TestConcurrentRedeemIsIdempotent(t *testing.T) {
	svc, ctx := setup(t)
	doc := uuid.New()
	if _, err := svc.EnsureOwner(ctx, doc, "alice"); err != nil {
		t.Fatal(err)
	}
	_, tok, err := svc.CreateLink(ctx, "alice", doc, perms.RoleEditor, time.Now().Add(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	errs := make(chan error, 16)
	for range 16 {
		go func() {
			_, err := svc.Redeem(ctx, "dave", tok)
			errs <- err
		}()
	}
	for range 16 {
		if err := <-errs; err != nil {
			t.Fatalf("concurrent redeem: %v", err)
		}
	}
	g, err := svc.Lookup(ctx, doc, "dave")
	if err != nil || g.Role != perms.RoleEditor {
		t.Fatalf("lookup: %+v %v", g, err)
	}
}

func TestRedeemLinkFailures(t *testing.T) {
	svc, ctx := setup(t)
	doc := uuid.New()
	if _, err := svc.EnsureOwner(ctx, doc, "alice"); err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 11, 5, 12, 0, 0, 0, time.UTC)
	svc.SetNow(func() time.Time { return now })
	l, tok, err := svc.CreateLink(ctx, "alice", doc, perms.RoleEditor, now.Add(time.Hour))
	if err != nil {
		t.Fatal(err)
	}

	if _, err := svc.Redeem(ctx, "bob", tok+"x"); !errors.Is(err, perms.ErrLinkBad) {
		t.Fatalf("tamper: %v", err)
	}
	raw := []byte(tok)
	raw[len(raw)-1] ^= 1
	if _, err := svc.Redeem(ctx, "bob", string(raw)); !errors.Is(err, perms.ErrLinkBad) {
		t.Fatalf("bit flip: %v", err)
	}

	if _, err := svc.RevokeLink(ctx, "alice", l.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := svc.Redeem(ctx, "bob", tok); !errors.Is(err, perms.ErrLinkRevoked) {
		t.Fatalf("revoked: %v", err)
	}
	if _, err := svc.Lookup(ctx, doc, "bob"); !errors.Is(err, perms.ErrNotFound) {
		t.Fatalf("revoked still granted: %v", err)
	}

	expiredID := uuid.New()
	exp := now.Add(-time.Minute)
	if err := svc.PutLink(ctx, perms.Link{ID: expiredID, DocID: doc, Role: perms.RoleViewer, ExpiresAt: exp, CreatedBy: "alice"}); err != nil {
		t.Fatal(err)
	}
	expiredTok, err := perms.SignLink(svc.ShareKey(), expiredID, doc, perms.RoleViewer, exp)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := svc.Redeem(ctx, "dave", expiredTok); !errors.Is(err, perms.ErrLinkExpired) {
		t.Fatalf("expired: %v", err)
	}

	missing, err := perms.SignLink(svc.ShareKey(), uuid.New(), doc, perms.RoleViewer, now.Add(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := svc.Redeem(ctx, "eve", missing); !errors.Is(err, perms.ErrLinkBad) {
		t.Fatalf("missing: %v", err)
	}
}

func TestPolicyDump(t *testing.T) {
	svc, ctx := setup(t)
	doc := uuid.New()
	p, err := svc.PolicyDump(ctx, doc)
	if err != nil || len(p.Members) != 0 || p.WriteEpoch != 0 || p.EpochState != "" {
		t.Fatalf("empty: %+v %v", p, err)
	}
	if _, err := svc.EnsureOwner(ctx, doc, "alice"); err != nil {
		t.Fatal(err)
	}
	if _, err := svc.Grant(ctx, "alice", doc, "bob", perms.RoleViewer); err != nil {
		t.Fatal(err)
	}
	p, err = svc.PolicyDump(ctx, doc)
	if err != nil || p.Members["alice"] != perms.RoleOwner || p.Members["bob"] != perms.RoleViewer {
		t.Fatalf("members: %+v %v", p, err)
	}
	if p.WriteEpoch != 0 || p.EpochState != "" {
		t.Fatalf("epoch %+v", p)
	}
}

type flakyEpochs struct {
	mu    sync.Mutex
	fail  int
	tries int
	ok    [][3]string
}

func (f *flakyEpochs) SetEpoch(_ context.Context, doc uuid.UUID, epoch uint32, state string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.tries++
	if f.fail > 0 {
		f.fail--
		return errors.New("epoch client down")
	}
	f.ok = append(f.ok, [3]string{doc.String(), strconv.FormatUint(uint64(epoch), 10), state})
	return nil
}

func (f *flakyEpochs) saw(doc uuid.UUID, epoch uint32, state string) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	want := [3]string{doc.String(), strconv.FormatUint(uint64(epoch), 10), state}
	for _, got := range f.ok {
		if got == want {
			return true
		}
	}
	return false
}

func (f *flakyEpochs) attempts() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.tries
}

func TestEpochSyncConvergesAfterPushFailures(t *testing.T) {
	svc, ctx := setup(t)
	epochs := &flakyEpochs{fail: 4}
	svc.UseEpochs(epochs)
	doc := uuid.New()
	if _, err := svc.EnsureDocument(ctx, doc, "alice", true); err != nil {
		t.Fatal(err)
	}
	bobKey, err := svc.PutKey(ctx, "bob", bytes.Repeat([]byte{2}, 32))
	if err != nil {
		t.Fatal(err)
	}
	wrap := perms.KeyWrap{
		Epoch: 1, UserID: "bob", KeyID: bobKey.ID,
		EphPub: bytes.Repeat([]byte{3}, 32), Nonce: bytes.Repeat([]byte{4}, 12), Ciphertext: bytes.Repeat([]byte{5}, 48),
	}
	if _, err := svc.GrantWithWraps(ctx, "alice", doc, "bob", perms.RoleEditor, []perms.KeyWrap{wrap}); err != nil {
		t.Fatal(err)
	}
	syncCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go svc.RunEpochSync(syncCtx)
	if err := svc.Revoke(ctx, "alice", doc, "bob"); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(8 * time.Second)
	for time.Now().Before(deadline) && !epochs.saw(doc, 2, "pending") {
		time.Sleep(100 * time.Millisecond)
	}
	if !epochs.saw(doc, 2, "pending") {
		t.Fatal("document-service did not converge on the pending epoch")
	}
	before := epochs.attempts()
	if err := svc.Revoke(ctx, "alice", doc, "bob"); !errors.Is(err, perms.ErrNotFound) {
		t.Fatalf("second revoke: %v", err)
	}
	if epochs.attempts() <= before {
		t.Fatal("revoking a missing member did not push")
	}
}

func setup(t *testing.T) (*perms.Service, context.Context) {
	t.Helper()
	pool, share := testdb.Open(t)
	return perms.New(pool, share), context.Background()
}
