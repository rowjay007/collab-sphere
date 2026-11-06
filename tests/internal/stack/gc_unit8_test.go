package stack

import (
	"context"
	"crypto/ecdh"
	"crypto/rand"
	"encoding/hex"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/encoding/protojson"

	commentv1 "github.com/rowjay007/collab-sphere/gen/comment/v1"
	crdtv1 "github.com/rowjay007/collab-sphere/gen/crdt/v1"
	documentv1 "github.com/rowjay007/collab-sphere/gen/document/v1"
	permissionv1 "github.com/rowjay007/collab-sphere/gen/permission/v1"
	versionv1 "github.com/rowjay007/collab-sphere/gen/version/v1"
	"github.com/rowjay007/collab-sphere/pkg/auth"
	"github.com/rowjay007/collab-sphere/pkg/collabclient"
	"github.com/rowjay007/collab-sphere/pkg/crdt"
	"github.com/rowjay007/collab-sphere/pkg/docrypt"
)

func waitLiveCaughtUp(t *testing.T, ctx context.Context, s *Stack, docID string) {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		doc, err := s.Docs.GetDocument(ctx, &documentv1.GetDocumentRequest{Id: docID})
		if err != nil {
			t.Fatal(err)
		}
		resp, err := s.Docs.ReadOperations(ctx, &documentv1.ReadOperationsRequest{Id: docID, AfterSeq: 0, Limit: 1})
		if err == nil && resp.GetLiveSeq() >= doc.GetOpSeq() {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		case <-time.After(200 * time.Millisecond):
		}
	}
	t.Fatal("live snapshot behind op_seq")
}

func waitLiveState(t *testing.T, ctx context.Context, s *Stack, docID string, ok func(*crdt.Doc) bool) {
	t.Helper()
	waitLiveGC(t, ctx, s, docID)
	waitLiveCaughtUp(t, ctx, s, docID)
	deadline := time.Now().Add(45 * time.Second)
	for time.Now().Before(deadline) {
		doc, _ := loadState(t, ctx, s.Docs, docID)
		if ok(doc) {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		case <-time.After(250 * time.Millisecond):
		}
	}
	t.Fatal("live state condition not met")
}

func liveDoc(t *testing.T, dbURL, docID string) *crdt.Doc {
	t.Helper()
	raw := liveSnapshotJSON(t, dbURL, docID)
	var snap crdtv1.Snapshot
	if err := protojson.Unmarshal([]byte(raw), &snap); err != nil {
		t.Fatal(err)
	}
	doc, err := crdt.FromSnapshot(&snap)
	if err != nil {
		t.Fatal(err)
	}
	return doc
}

func liveSnapshotJSON(t *testing.T, dbURL, docID string) string {
	t.Helper()
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, dbURL)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close(ctx)
	var raw []byte
	if err := conn.QueryRow(ctx, `SELECT state FROM snapshots WHERE document_id = $1::uuid AND kind = 'live'`, docID).Scan(&raw); err != nil {
		t.Fatal(err)
	}
	return string(raw)
}

func ageAllButTipVersion(t *testing.T, pool *pgxpool.Pool, docID string) {
	t.Helper()
	ctx := context.Background()
	if _, err := pool.Exec(ctx, `
		UPDATE nodes
		SET created_at = now() - interval '800 hours'
		WHERE doc = $1::uuid
		  AND seq < (SELECT max(seq) FROM nodes WHERE doc = $1::uuid)`,
		docID); err != nil {
		t.Fatal(err)
	}
}

func waitForkReport(t *testing.T, ctx context.Context, s *Stack, forkRoot string, replica uint64) {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		resp, err := s.Docs.ListReports(ctx, &documentv1.ListReportsRequest{ForkRoot: forkRoot})
		if err != nil {
			t.Fatal(err)
		}
		for _, r := range resp.GetReports() {
			if r.GetReplicaId() == replica {
				return
			}
		}
		select {
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		case <-time.After(100 * time.Millisecond):
		}
	}
	t.Fatalf("replica %d report missing for fork %s", replica, forkRoot)
}

func ageAllReplicaReports(t *testing.T, dbURL, forkRoot string, by time.Duration) {
	t.Helper()
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, dbURL)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close(ctx)
	if _, err := conn.Exec(ctx, `
		UPDATE replica_reports
		SET reported_at = now() - $2::interval
		WHERE fork_root = $1::uuid`,
		forkRoot, by.String()); err != nil {
		t.Fatal(err)
	}
}

func opIDProto(id crdt.ID) *crdtv1.OpID {
	return &crdtv1.OpID{Client: uint64(id.Client), Clock: id.Clock}
}

func TestGCBranchReplicaProtectsLeafUntilRetired(t *testing.T) {
	s := New(t, WithExport())
	ctx, cancel := context.WithTimeout(t.Context(), 90*time.Second)
	defer cancel()
	restartDocumentGC(t, s, 30*time.Minute)

	docID := s.CreateDocument(ctx)
	meta, err := s.Docs.GetDocument(ctx, &documentv1.GetDocumentRequest{Id: docID})
	if err != nil {
		t.Fatal(err)
	}
	forkRoot := meta.GetForkRoot()
	onD := s.dial(ctx, docID, collabclient.Options{URLs: []string{s.HubURL(0)}, Client: 1})
	var control, nodeN crdt.ID
	if err := onD.Edit(func(r *crdt.Replica) ([]crdt.Op, error) {
		w, err := r.InsertAfter(crdt.Root, 'w')
		if err != nil {
			return nil, err
		}
		del, err := r.Delete(w.ID)
		if err != nil {
			return nil, err
		}
		x, err := r.InsertAfter(crdt.Root, 'x')
		if err != nil {
			return nil, err
		}
		control, nodeN = w.ID, x.ID
		return []crdt.Op{w, del, x}, nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := onD.WaitAcked(ctx); err != nil {
		t.Fatal(err)
	}
	head, err := s.Versions.Client.Checkpoint(ctx, &versionv1.CheckpointRequest{
		DocId: docID, Name: "branch-at", RequestId: t.Name() + "-cp",
	})
	if err != nil {
		t.Fatal(err)
	}
	branch, err := s.Versions.Client.Branch(ctx, &versionv1.BranchRequest{
		VersionId: head.GetId(), Title: "B", RequestId: t.Name() + "-branch",
	})
	if err != nil {
		t.Fatal(err)
	}
	bID := branch.GetDocumentId()
	onB := s.DialAllocated(ctx, bID, 0)
	if err := onB.WaitAcked(ctx); err != nil {
		t.Fatal(err)
	}
	replicaR := onB.ID()
	if err := onB.Edit(func(r *crdt.Replica) ([]crdt.Op, error) {
		y, err := r.InsertAfter(nodeN, 'y')
		if err != nil {
			return nil, err
		}
		return []crdt.Op{y}, nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := onB.WaitAcked(ctx); err != nil {
		t.Fatal(err)
	}
	waitForkReport(t, ctx, s, forkRoot, uint64(replicaR))
	onB.Disconnect()
	if _, err := s.Docs.ArchiveDocument(s.AuthCtx(ctx, s.Owner()), &documentv1.ArchiveDocumentRequest{Id: bID}); err != nil {
		t.Fatal(err)
	}
	if err := onD.DeleteAt(0); err != nil {
		t.Fatal(err)
	}
	if err := onD.WaitAcked(ctx); err != nil {
		t.Fatal(err)
	}
	onD.Disconnect()
	if _, err := s.Versions.Client.Checkpoint(ctx, &versionv1.CheckpointRequest{
		DocId: docID, Name: "post-delete", RequestId: t.Name() + "-cp2",
	}); err != nil {
		t.Fatal(err)
	}
	ageAllButTipVersion(t, s.Versions.DB(), docID)
	ageReportsExcept(t, s.dbURL, forkRoot, uint64(replicaR), 31*time.Minute)

	waitLiveState(t, ctx, s, docID, func(*crdt.Doc) bool {
		return !liveDoc(t, s.dbURL, docID).Has(control)
	})
	if !liveDoc(t, s.dbURL, docID).Has(nodeN) {
		t.Fatal("D collected the parent of B's insert while B's replica report is live")
	}

	ageReplicaReport(t, s.dbURL, forkRoot, uint64(replicaR), 31*time.Minute)
	waitLiveState(t, ctx, s, docID, func(*crdt.Doc) bool {
		return !liveDoc(t, s.dbURL, docID).Has(nodeN)
	})
}

func ageReportsExcept(t *testing.T, dbURL, forkRoot string, keep uint64, by time.Duration) {
	t.Helper()
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, dbURL)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close(ctx)
	if _, err := conn.Exec(ctx, `
		UPDATE replica_reports
		SET reported_at = now() - $3::interval
		WHERE fork_root = $1::uuid AND replica_id <> $2::numeric`,
		forkRoot, keep, by.String()); err != nil {
		t.Fatal(err)
	}
}

func TestHorizonPinsSurviveDeletedRange(t *testing.T) {
	s := New(t, WithExport())
	ctx, cancel := context.WithTimeout(t.Context(), 90*time.Second)
	defer cancel()
	commentAddr := restartDocumentGC(t, s, 30*time.Minute)

	docID := s.CreateDocument(ctx)
	meta, err := s.Docs.GetDocument(ctx, &documentv1.GetDocumentRequest{Id: docID})
	if err != nil {
		t.Fatal(err)
	}
	conn, err := grpc.NewClient(commentAddr, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithUnaryInterceptor(s.commentAuth()))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	cs := commentv1.NewCommentServiceClient(conn)
	c := s.dial(ctx, docID, collabclient.Options{URLs: []string{s.HubURL(0)}, Client: 1})
	var pinned, stray crdt.ID
	if err := c.Edit(func(r *crdt.Replica) ([]crdt.Op, error) {
		left, err := r.InsertAfter(crdt.Root, 'a')
		if err != nil {
			return nil, err
		}
		right, err := r.InsertAfter(crdt.Root, 'b')
		if err != nil {
			return nil, err
		}
		pinned = left.ID
		stray = right.ID
		return []crdt.Op{left, right}, nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := c.WaitAcked(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := cs.CreateThread(s.AuthCtx(ctx, s.Owner()), &commentv1.CreateThreadRequest{
		DocumentId: docID,
		Anchor:     &commentv1.Anchor{Start: opIDProto(pinned), End: opIDProto(pinned)},
		Author:     s.Owner(),
		Body:       "pinned",
		RequestId:  t.Name() + "-thread",
	}); err != nil {
		t.Fatal(err)
	}
	internalConn, err := grpc.NewClient(commentAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithUnaryInterceptor(auth.InternalUnary(s.internal)),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer internalConn.Close()
	internalCS := commentv1.NewCommentServiceClient(internalConn)
	pinsResp, err := internalCS.AnchorPins(s.InternalCtx(ctx), &commentv1.AnchorPinsRequest{ForkRoot: meta.GetForkRoot()})
	if err != nil {
		t.Fatal(err)
	}
	if len(pinsResp.GetPins()) == 0 {
		t.Fatal("anchor pins empty")
	}
	pinOK := false
	for _, p := range pinsResp.GetPins() {
		id := crdt.ID{Client: crdt.ClientID(p.GetClient()), Clock: p.GetClock()}
		if id == pinned {
			pinOK = true
			break
		}
	}
	if !pinOK {
		t.Fatalf("pins omit anchor %v", pinned)
	}
	if err := c.Edit(func(r *crdt.Replica) ([]crdt.Op, error) {
		d1, err := r.Delete(pinned)
		if err != nil {
			return nil, err
		}
		d2, err := r.Delete(stray)
		if err != nil {
			return nil, err
		}
		return []crdt.Op{d1, d2}, nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := c.WaitAcked(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Versions.Client.Checkpoint(ctx, &versionv1.CheckpointRequest{
		DocId: docID, Name: "pins", RequestId: t.Name() + "-cp",
	}); err != nil {
		t.Fatal(err)
	}
	ageAllButTipVersion(t, s.Versions.DB(), docID)
	c.Disconnect()
	ageAllReplicaReports(t, s.dbURL, meta.GetForkRoot(), 31*time.Minute)
	waitLiveState(t, ctx, s, docID, func(*crdt.Doc) bool {
		d := liveDoc(t, s.dbURL, docID)
		return d.Has(pinned) && !d.Has(stray)
	})
	raw := liveSnapshotJSON(t, s.dbURL, docID)
	if strings.Contains(raw, stray.String()) {
		t.Fatal("live snapshot still contains stray id")
	}
}

func TestGCEncryptedDocKeepsCipherInLog(t *testing.T) {
	s := New(t, WithExport())
	ctx, cancel := context.WithTimeout(t.Context(), 90*time.Second)
	defer cancel()
	restartDocumentGC(t, s, 2*time.Hour)

	marker := encryptedMarker(t)
	markerRunes := []rune(marker)
	docID := createEncryptedDoc(t, ctx, s)
	doc := uuid.MustParse(docID)
	owner := encryptedMember(t, ctx, s, s.Owner())
	key := encryptedDocKey(t)
	if _, err := s.Perm.Client.PublishWraps(s.AuthCtx(ctx, s.Owner()), &permissionv1.PublishWrapsRequest{
		DocId: docID, RequestId: t.Name() + "-open",
		Wraps: []*permissionv1.KeyWrap{owner.wrap(t, doc, 1, key)},
	}); err != nil {
		t.Fatal(err)
	}
	var epoch atomic.Uint32
	epoch.Store(1)
	keys := map[uint32][]byte{1: key}
	client := dialEncryptedDoc(t, ctx, s, docID, s.Owner(), 1, &epoch, keys)
	for _, r := range markerRunes {
		if err := client.InsertAt(0, r); err != nil {
			t.Fatal(err)
		}
	}
	if err := client.WaitAcked(ctx); err != nil {
		t.Fatal(err)
	}
	var collected []crdt.ID
	client.Read(func(r *crdt.Replica) {
		for i := range r.Len() {
			id, err := r.IDAt(i)
			if err != nil {
				t.Fatal(err)
			}
			collected = append(collected, id)
		}
	})
	if len(collected) != len(markerRunes) {
		t.Fatalf("visible nodes = %d, want %d", len(collected), len(markerRunes))
	}
	for len([]rune(client.Text())) > 0 {
		if err := client.DeleteAt(0); err != nil {
			t.Fatal(err)
		}
	}
	if err := client.WaitAcked(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Versions.Client.Checkpoint(ctx, &versionv1.CheckpointRequest{
		DocId: docID, Name: "enc-gc", RequestId: t.Name() + "-cp",
	}); err != nil {
		t.Fatal(err)
	}
	ageAllButTipVersion(t, s.Versions.DB(), docID)
	client.Disconnect()
	meta, err := s.Docs.GetDocument(ctx, &documentv1.GetDocumentRequest{Id: docID})
	if err != nil {
		t.Fatal(err)
	}
	ageAllReplicaReports(t, s.dbURL, meta.GetForkRoot(), 3*time.Hour)
	opsBefore := operationCount(t, s.dbURL, docID)

	waitLiveState(t, ctx, s, docID, func(*crdt.Doc) bool {
		live := liveDoc(t, s.dbURL, docID)
		for _, id := range collected {
			if live.Has(id) {
				return false
			}
		}
		return true
	})
	if operationCount(t, s.dbURL, docID) != opsBefore {
		t.Fatal("operations count changed after gc")
	}
	logged := make(map[crdt.ID]*crdt.Ciphertext)
	resp, err := s.Docs.ReadOperations(ctx, &documentv1.ReadOperationsRequest{Id: docID, AfterSeq: 0, Limit: 1000})
	if err != nil {
		t.Fatal(err)
	}
	for _, p := range resp.GetOperations() {
		op, err := crdt.OpFromProto(p)
		if err != nil {
			t.Fatal(err)
		}
		if ins, ok := op.(crdt.Insert); ok {
			logged[ins.ID] = ins.Cipher
		}
	}
	for _, id := range collected {
		cipher, ok := logged[id]
		if !ok {
			t.Fatalf("collected insert %v missing from operations", id)
		}
		if cipher == nil || len(cipher.Bytes) == 0 {
			t.Fatalf("collected insert %v lost its ciphertext in operations", id)
		}
	}
	raw := liveSnapshotJSON(t, s.dbURL, docID)
	if strings.Contains(raw, marker) {
		t.Fatalf("live snapshot contains marker %q", marker)
	}
	var snap crdtv1.Snapshot
	if err := protojson.Unmarshal([]byte(raw), &snap); err != nil {
		t.Fatal(err)
	}
	for _, n := range snap.GetNodes() {
		if n.GetValue() != 0 {
			t.Fatalf("live node value=%d", n.GetValue())
		}
	}
}

func encryptedMarker(t *testing.T) string {
	t.Helper()
	var raw [20]byte
	if _, err := rand.Read(raw[:]); err != nil {
		t.Fatal(err)
	}
	return hex.EncodeToString(raw[:]) + "ZZ"
}

type encMember struct {
	user  string
	priv  *ecdh.PrivateKey
	keyID string
}

func (m encMember) wrap(t *testing.T, doc uuid.UUID, epoch uint32, docKey []byte) *permissionv1.KeyWrap {
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

func encryptedMember(t *testing.T, ctx context.Context, s *Stack, user string) encMember {
	t.Helper()
	priv, err := ecdh.X25519().GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	out, err := s.Perm.Client.PutKey(s.AuthCtx(ctx, user), &permissionv1.PutKeyRequest{PublicKey: priv.PublicKey().Bytes()})
	if err != nil {
		t.Fatal(err)
	}
	return encMember{user: user, priv: priv, keyID: out.GetKeyId()}
}

func encryptedDocKey(t *testing.T) []byte {
	t.Helper()
	k := make([]byte, 32)
	if _, err := rand.Read(k); err != nil {
		t.Fatal(err)
	}
	return k
}

func createEncryptedDoc(t *testing.T, ctx context.Context, s *Stack) string {
	t.Helper()
	doc, err := s.Docs.CreateDocument(s.AuthCtx(ctx, s.Owner()), &documentv1.CreateDocumentRequest{
		Title: "sealed", OwnerId: s.Owner(), Encrypted: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	return doc.GetId()
}

func dialEncryptedDoc(t *testing.T, ctx context.Context, s *Stack, docID, user string, client crdt.ClientID, epoch *atomic.Uint32, keys map[uint32][]byte) *collabclient.Client {
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
