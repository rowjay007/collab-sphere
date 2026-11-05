package e2e

import (
	"context"
	"crypto/ecdh"
	"crypto/rand"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	collabv1 "github.com/rowjay007/collab-sphere/gen/collab/v1"
	commentv1 "github.com/rowjay007/collab-sphere/gen/comment/v1"
	crdtv1 "github.com/rowjay007/collab-sphere/gen/crdt/v1"
	documentv1 "github.com/rowjay007/collab-sphere/gen/document/v1"
	exportv1 "github.com/rowjay007/collab-sphere/gen/export/v1"
	permissionv1 "github.com/rowjay007/collab-sphere/gen/permission/v1"
	versionv1 "github.com/rowjay007/collab-sphere/gen/version/v1"
	"github.com/rowjay007/collab-sphere/pkg/collabclient"
	"github.com/rowjay007/collab-sphere/pkg/crdt"
	"github.com/rowjay007/collab-sphere/pkg/docrypt"
	"github.com/rowjay007/collab-sphere/tests/internal/stack"
)

func TestEncryptedDocuments(t *testing.T) {
	s := stack.New(t, stack.WithExport())
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	cs := stack.StartCommentService(s, s.Versions)

	docID := createEncrypted(t, ctx, s)
	doc := uuid.MustParse(docID)
	owner := memberKey(t, ctx, s, s.Owner())
	bob := memberKey(t, ctx, s, "bob")
	carol := memberKey(t, ctx, s, "carol")
	key1 := randomKey(t)

	grantWraps(t, ctx, s, doc, s.Owner(), "bob", permissionv1.Role_ROLE_EDITOR, []member{bob}, 1, key1)
	pub, err := s.Perm.Client.PublishWraps(s.AuthCtx(ctx, s.Owner()), &permissionv1.PublishWrapsRequest{
		DocId:     docID,
		RequestId: t.Name() + "-open-1",
		Wraps:     []*permissionv1.KeyWrap{owner.wrap(t, doc, 1, key1), bob.wrap(t, doc, 1, key1)},
	})
	if err != nil || pub.GetWriteEpoch() != 1 || pub.GetEpochState() != "open" {
		t.Fatalf("publish: %+v %v", pub, err)
	}
	opened, err := s.Docs.GetDocument(s.AuthCtx(ctx, s.Owner()), &documentv1.GetDocumentRequest{Id: docID})
	if err != nil || opened.GetEncryption() != "encrypted" || opened.GetWriteEpoch() != 1 || opened.GetEpochState() != "open" {
		t.Fatalf("document epoch: %+v %v", opened, err)
	}

	var epoch atomic.Uint32
	epoch.Store(1)
	aliceKeys := map[uint32][]byte{1: mustUnwrap(t, ctx, s, doc, owner, 1)}
	bobKeys := map[uint32][]byte{1: mustUnwrap(t, ctx, s, doc, bob, 1)}
	alice := dialEncrypted(t, ctx, s, docID, s.Owner(), 0, &epoch, aliceKeys)
	editor := dialEncrypted(t, ctx, s, docID, "bob", 0, &epoch, bobKeys)
	fresh := dialEncrypted(t, ctx, s, docID, s.Owner(), 99, &epoch, aliceKeys)
	if fresh.ID() == 99 {
		t.Fatal("encrypted client resumed a replica it has no clock for")
	}
	fresh.Close()

	insertText(t, alice, "Ab")
	waitOpened(t, editor, "Ab")
	version, err := s.Versions.Client.Checkpoint(ctx, &versionv1.CheckpointRequest{DocId: docID, Name: "v1", RequestId: t.Name() + "-v1"})
	if err != nil {
		t.Fatal(err)
	}
	insertText(t, alice, "C")
	waitOpened(t, alice, "AbC")
	waitOpened(t, editor, "AbC")
	if _, err := s.Versions.Client.Restore(ctx, &versionv1.RestoreRequest{DocId: docID, VersionId: version.GetId(), RequestId: t.Name() + "-restore"}); err != nil {
		t.Fatal(err)
	}
	waitOpened(t, alice, "Ab")
	waitOpened(t, editor, "Ab")

	if _, err := collabclient.DialWith(ctx, docID, collabclient.Options{URLs: []string{s.HubURL(0)}, Token: s.Token("carol")}); err == nil {
		t.Fatal("carol joined without a grant")
	}

	grantWraps(t, ctx, s, doc, s.Owner(), "carol", permissionv1.Role_ROLE_EDITOR, []member{carol}, 1, key1)
	carolKeys := map[uint32][]byte{1: mustUnwrap(t, ctx, s, doc, carol, 1)}
	reader := dialEncrypted(t, ctx, s, docID, "carol", 0, &epoch, carolKeys)
	waitOpened(t, reader, "Ab")

	var anchor crdt.ID
	alice.Read(func(r *crdt.Replica) {
		var err error
		anchor, err = r.IDAt(0)
		if err != nil {
			t.Fatal(err)
		}
	})
	if _, err := cs.CreateThread(s.AuthCtx(ctx, s.Owner()), &commentv1.CreateThreadRequest{
		DocumentId: docID,
		Anchor:     &commentv1.Anchor{Start: opID(anchor), End: opID(anchor)},
		Author:     s.Owner(),
		Body:       "note",
		RequestId:  t.Name() + "-thread",
	}); err != nil {
		t.Fatal(err)
	}
	threads, err := cs.ListThreads(s.AuthCtx(ctx, s.Owner()), &commentv1.ListThreadsRequest{DocumentId: docID})
	if err != nil || len(threads.GetThreads()) != 1 {
		t.Fatalf("threads: %+v %v", threads, err)
	}
	res := threads.GetThreads()[0].GetResolution()
	if res.GetText() != "" || !res.GetTextOmitted() {
		t.Fatalf("resolution text %q omitted %v", res.GetText(), res.GetTextOmitted())
	}

	link, err := s.Perm.Client.CreateLink(s.AuthCtx(ctx, s.Owner()), &permissionv1.CreateLinkRequest{
		DocId: docID, Role: permissionv1.Role_ROLE_VIEWER, ExpiresAt: timestamppb.New(time.Now().Add(time.Hour)),
	})
	if err != nil {
		t.Fatal(err)
	}
	_, err = s.Perm.Client.Redeem(s.AuthCtx(ctx, "dave"), &permissionv1.RedeemRequest{Token: link.GetToken()})
	if status.Code(err) != codes.FailedPrecondition || linkReason(err) != "ENCRYPTED" {
		t.Fatalf("redeem: %v", err)
	}

	_, err = s.Docs.ForkDocument(ctx, &documentv1.ForkDocumentRequest{SourceId: docID, Seq: opened.GetOpSeq(), Title: "fork", RequestId: t.Name() + "-fork"})
	if status.Code(err) != codes.FailedPrecondition || linkReason(err) != "ENCRYPTED" {
		t.Fatalf("fork: %v", err)
	}
	_, err = s.Versions.Client.Branch(ctx, &versionv1.BranchRequest{VersionId: version.GetId(), Title: "branch", RequestId: t.Name() + "-branch"})
	if status.Code(err) != codes.FailedPrecondition || linkReason(err) != "ENCRYPTED" {
		t.Fatalf("branch: %v", err)
	}
	_, err = s.Versions.Client.Merge(ctx, &versionv1.MergeRequest{SourceDocId: docID, TargetDocId: docID, RequestId: t.Name() + "-merge"})
	if status.Code(err) != codes.FailedPrecondition || linkReason(err) != "ENCRYPTED" {
		t.Fatalf("merge: %v", err)
	}
	_, err = s.Versions.Client.Diff(ctx, &versionv1.DiffRequest{FromVersionId: version.GetId(), ToVersionId: version.GetId()})
	if status.Code(err) != codes.FailedPrecondition || linkReason(err) != "ENCRYPTED" {
		t.Fatalf("diff: %v", err)
	}
	_, err = s.Export.Client.RequestExport(s.AuthCtx(ctx, s.Owner()), &exportv1.RequestExportRequest{DocId: docID, Format: "md", RequestId: t.Name() + "-export"})
	if status.Code(err) != codes.FailedPrecondition || linkReason(err) != "ENCRYPTED" {
		t.Fatalf("export: %v", err)
	}

	if _, err := s.Perm.Client.Revoke(s.AuthCtx(ctx, s.Owner()), &permissionv1.RevokeRequest{DocId: docID, UserId: "carol"}); err != nil {
		t.Fatal(err)
	}
	time.Sleep(2 * time.Second)
	if err := editor.InsertAt(editorLen(t, editor), 'X'); err != nil {
		t.Fatal(err)
	}
	waitCode(t, editor, collabv1.ErrorMessage_CODE_KEY_ROTATION)
	if !editor.Connected() {
		t.Fatal("key rotation closed the editor")
	}
	_, err = s.Versions.Client.Restore(ctx, &versionv1.RestoreRequest{DocId: docID, VersionId: version.GetId(), RequestId: t.Name() + "-restore-pending"})
	if status.Code(err) != codes.FailedPrecondition || linkReason(err) != "KEY_ROTATION" {
		t.Fatalf("restore while pending: %v", err)
	}

	key2 := randomKey(t)
	pub2, err := s.Perm.Client.PublishWraps(s.AuthCtx(ctx, s.Owner()), &permissionv1.PublishWrapsRequest{
		DocId:     docID,
		RequestId: t.Name() + "-open-2",
		Wraps:     []*permissionv1.KeyWrap{owner.wrap(t, doc, 2, key2), bob.wrap(t, doc, 2, key2)},
	})
	if err != nil || pub2.GetWriteEpoch() != 2 || pub2.GetEpochState() != "open" {
		t.Fatalf("publish 2: %+v %v", pub2, err)
	}
	again, err := s.Perm.Client.PublishWraps(s.AuthCtx(ctx, s.Owner()), &permissionv1.PublishWrapsRequest{
		DocId:     docID,
		RequestId: t.Name() + "-open-1",
		Wraps:     []*permissionv1.KeyWrap{owner.wrap(t, doc, 1, key1), bob.wrap(t, doc, 1, key1)},
	})
	if err != nil || again.GetWriteEpoch() != 1 || again.GetEpochState() != "open" {
		t.Fatalf("replay: %+v %v", again, err)
	}
	_, err = s.Perm.Client.PublishWraps(s.AuthCtx(ctx, s.Owner()), &permissionv1.PublishWrapsRequest{
		DocId:     docID,
		RequestId: t.Name() + "-other",
		Wraps:     []*permissionv1.KeyWrap{owner.wrap(t, doc, 2, key2), bob.wrap(t, doc, 2, key2)},
	})
	if status.Code(err) != codes.AlreadyExists {
		t.Fatalf("second publish: %v", err)
	}
	if _, err := s.Versions.Client.Restore(ctx, &versionv1.RestoreRequest{DocId: docID, VersionId: version.GetId(), RequestId: t.Name() + "-restore-open"}); err != nil {
		t.Fatalf("restore after publish: %v", err)
	}

	aliceKeys[2] = mustUnwrap(t, ctx, s, doc, owner, 2)
	bobKeys[2] = mustUnwrap(t, ctx, s, doc, bob, 2)
	epoch.Store(2)
	time.Sleep(2 * time.Second)
	if err := alice.InsertAt(2, 'Z'); err != nil {
		t.Fatal(err)
	}
	waitOpened(t, alice, "AbZ")
	ackCtx, ackCancel := context.WithTimeout(ctx, 3*time.Second)
	defer ackCancel()
	if err := alice.WaitAcked(ackCtx); err != nil {
		t.Fatal(err)
	}

	if _, err := s.Perm.Client.GetWraps(s.AuthCtx(ctx, "carol"), &permissionv1.GetWrapsRequest{DocId: docID}); status.Code(err) != codes.PermissionDenied {
		t.Fatalf("carol wraps: %v", err)
	}
	var sawOld, sawNew bool
	ops, err := s.Docs.ReadOperations(ctx, &documentv1.ReadOperationsRequest{Id: docID, Limit: 1000})
	if err != nil {
		t.Fatal(err)
	}
	for _, raw := range ops.GetOperations() {
		op, err := crdt.OpFromProto(raw)
		if err != nil {
			t.Fatal(err)
		}
		ins, ok := op.(crdt.Insert)
		if !ok || ins.Cipher == nil {
			continue
		}
		_, openErr := docrypt.Open(key1, doc, ins.Cipher.Epoch, ins.ID, ins.Cipher.Bytes)
		switch ins.Cipher.Epoch {
		case 1:
			if openErr != nil {
				t.Fatalf("epoch 1 op: %v", openErr)
			}
			sawOld = true
		case 2:
			if openErr == nil {
				t.Fatal("removed client opened an epoch 2 insert")
			}
			if _, err := docrypt.Open(key2, doc, ins.Cipher.Epoch, ins.ID, ins.Cipher.Bytes); err != nil {
				t.Fatalf("owner key did not open epoch 2: %v", err)
			}
			sawNew = true
		}
	}
	if !sawOld || !sawNew {
		t.Fatalf("ops old %v new %v", sawOld, sawNew)
	}

	_, err = s.Docs.AppendOperations(ctx, &documentv1.AppendOperationsRequest{
		Id: docID,
		Operations: []*crdtv1.Operation{{
			Id: &crdtv1.OpID{Client: 77, Clock: 1},
			Kind: &crdtv1.Operation_Insert{Insert: &crdtv1.Insert{
				Parent: &crdtv1.OpID{},
				Cipher: []byte("0123456789abcdef0"),
				Epoch:  1,
			}},
		}},
	})
	if err == nil {
		t.Fatal("append accepted a stale epoch")
	}
}

type member struct {
	user  string
	priv  *ecdh.PrivateKey
	keyID string
}

func (m member) wrap(t *testing.T, doc uuid.UUID, epoch uint32, docKey []byte) *permissionv1.KeyWrap {
	t.Helper()
	sealed, err := docrypt.WrapKey(docKey, doc, epoch, m.user, uuid.MustParse(m.keyID), m.priv.PublicKey())
	if err != nil {
		t.Fatal(err)
	}
	return &permissionv1.KeyWrap{
		Epoch: epoch, UserId: m.user, KeyId: m.keyID,
		EphPub: sealed.EphPub, Nonce: sealed.Nonce, Ciphertext: sealed.Ciphertext,
	}
}

func memberKey(t *testing.T, ctx context.Context, s *stack.Stack, user string) member {
	t.Helper()
	priv, err := ecdh.X25519().GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	out, err := s.Perm.Client.PutKey(s.AuthCtx(ctx, user), &permissionv1.PutKeyRequest{PublicKey: priv.PublicKey().Bytes()})
	if err != nil {
		t.Fatal(err)
	}
	return member{user: user, priv: priv, keyID: out.GetKeyId()}
}

func randomKey(t *testing.T) []byte {
	t.Helper()
	k := make([]byte, 32)
	if _, err := rand.Read(k); err != nil {
		t.Fatal(err)
	}
	return k
}

func grantWraps(t *testing.T, ctx context.Context, s *stack.Stack, doc uuid.UUID, actor, user string, role permissionv1.Role, members []member, epoch uint32, docKey []byte) {
	t.Helper()
	var wraps []*permissionv1.KeyWrap
	for e := uint32(1); e <= epoch; e++ {
		for _, m := range members {
			if m.user == user {
				wraps = append(wraps, m.wrap(t, doc, e, docKey))
			}
		}
	}
	if _, err := s.Perm.Client.Grant(s.AuthCtx(ctx, actor), &permissionv1.GrantRequest{
		DocId: doc.String(), UserId: user, Role: role, Wraps: wraps,
	}); err != nil {
		t.Fatal(err)
	}
}

func mustUnwrap(t *testing.T, ctx context.Context, s *stack.Stack, doc uuid.UUID, m member, epoch uint32) []byte {
	t.Helper()
	resp, err := s.Perm.Client.GetWraps(s.AuthCtx(ctx, m.user), &permissionv1.GetWrapsRequest{DocId: doc.String()})
	if err != nil {
		t.Fatal(err)
	}
	for _, w := range resp.GetWraps() {
		if w.GetEpoch() != epoch {
			continue
		}
		key, err := docrypt.UnwrapKey(docrypt.Wrap{EphPub: w.GetEphPub(), Nonce: w.GetNonce(), Ciphertext: w.GetCiphertext()}, doc, epoch, m.user, uuid.MustParse(w.GetKeyId()), m.priv)
		if err != nil {
			t.Fatal(err)
		}
		return key
	}
	t.Fatalf("%s has no wrap for epoch %d", m.user, epoch)
	return nil
}

func createEncrypted(t *testing.T, ctx context.Context, s *stack.Stack) string {
	t.Helper()
	doc, err := s.Docs.CreateDocument(s.AuthCtx(ctx, s.Owner()), &documentv1.CreateDocumentRequest{
		Title: "sealed", OwnerId: s.Owner(), Encrypted: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if doc.GetEncryption() != "encrypted" || doc.GetWriteEpoch() != 1 || doc.GetEpochState() != "pending" {
		t.Fatalf("create: %+v", doc)
	}
	return doc.GetId()
}

func dialEncrypted(t *testing.T, ctx context.Context, s *stack.Stack, docID, user string, client crdt.ClientID, epoch *atomic.Uint32, keys map[uint32][]byte) *collabclient.Client {
	t.Helper()
	c, err := collabclient.DialWith(ctx, docID, collabclient.Options{
		URLs:   []string{s.HubURL(0)},
		Client: client,
		Token:  s.Token(user),
		DocID:  uuid.MustParse(docID),
		Key: func(e uint32) ([]byte, error) {
			k, ok := keys[e]
			if !ok {
				return nil, context.Canceled
			}
			return k, nil
		},
		WriteEpoch: func() uint32 { return epoch.Load() },
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.Close() })
	return c
}

func insertText(t *testing.T, c *collabclient.Client, text string) {
	t.Helper()
	for _, r := range text {
		if err := c.InsertAt(editorLen(t, c), r); err != nil {
			t.Fatal(err)
		}
	}
}

func editorLen(t *testing.T, c *collabclient.Client) int {
	t.Helper()
	n := 0
	c.Read(func(r *crdt.Replica) { n = r.Len() })
	return n
}

func waitOpened(t *testing.T, c *collabclient.Client, want string) {
	t.Helper()
	deadline := time.Now().Add(8 * time.Second)
	for time.Now().Before(deadline) {
		if c.Text() == want {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("text %q, want %q", c.Text(), want)
}
