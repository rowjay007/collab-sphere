package e2e

import (
	"context"
	"encoding/base64"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	collabv1 "github.com/rowjay007/collab-sphere/gen/collab/v1"
	commentv1 "github.com/rowjay007/collab-sphere/gen/comment/v1"
	crdtv1 "github.com/rowjay007/collab-sphere/gen/crdt/v1"
	documentv1 "github.com/rowjay007/collab-sphere/gen/document/v1"
	permissionv1 "github.com/rowjay007/collab-sphere/gen/permission/v1"
	"github.com/rowjay007/collab-sphere/pkg/collabclient"
	"github.com/rowjay007/collab-sphere/pkg/crdt"
	"github.com/rowjay007/collab-sphere/tests/internal/stack"
)

func TestAuthn(t *testing.T) {
	s := stack.New(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	docID := s.CreateDocument(ctx)
	raw := rawDocs(t, s.GRPCAddr())

	for _, tok := range []string{"", s.Mint("e2e", time.Now().Add(-2*time.Minute), time.Now().Add(-time.Minute)), flipSig(s.Token("e2e"))} {
		err := dialHub(ctx, s, docID, tok, 0)
		if err == nil || !strings.Contains(err.Error(), "401") {
			t.Fatalf("hub dial %q: %v", tok, err)
		}
		_, err = raw.CreateDocument(bearer(ctx, tok), &documentv1.CreateDocumentRequest{Title: "x"})
		if status.Code(err) != codes.Unauthenticated {
			t.Fatalf("CreateDocument %q: %v", tok, err)
		}
	}

	_, err := raw.ClaimReplica(s.AuthCtx(ctx, "e2e"), &documentv1.ClaimReplicaRequest{DocumentId: docID})
	if status.Code(err) != codes.Unauthenticated {
		t.Fatalf("user token on ClaimReplica: %v", err)
	}
	_, err = raw.CreateDocument(s.InternalCtx(ctx), &documentv1.CreateDocumentRequest{Title: "internal"})
	if status.Code(err) != codes.Unauthenticated {
		t.Fatalf("internal token on CreateDocument: %v", err)
	}
	_, err = s.Docs.CreateDocument(s.AuthCtx(ctx, "e2e"), &documentv1.CreateDocumentRequest{Title: "mismatch", OwnerId: "other"})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("owner mismatch: %v", err)
	}
}

func TestAuthz(t *testing.T) {
	s := stack.New(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	vs := stack.StartVersionService(s, 0)
	cs := stack.StartCommentService(s, vs)
	docID := s.CreateDocument(ctx)
	s.Grant(ctx, docID, "viewer", permissionv1.Role_ROLE_VIEWER)
	s.Grant(ctx, docID, "commenter", permissionv1.Role_ROLE_COMMENTER)
	s.Grant(ctx, docID, "editor", permissionv1.Role_ROLE_EDITOR)

	viewer := dialUser(t, ctx, s, docID, "viewer", 11)
	editor := dialUser(t, ctx, s, docID, "editor", 12)
	if err := editor.InsertAt(0, 'E'); err != nil {
		t.Fatal(err)
	}
	waitContains(t, viewer, "E")
	if err := viewer.InsertAt(0, 'V'); err != nil {
		t.Fatal(err)
	}
	waitCode(t, viewer, collabv1.ErrorMessage_CODE_FORBIDDEN)
	if !viewer.Connected() {
		t.Fatal("viewer socket closed after CODE_FORBIDDEN")
	}
	if editor.Text() != "E" {
		t.Fatalf("editor text %q, viewer op was stored", editor.Text())
	}

	var anchor crdt.ID
	editor.Read(func(r *crdt.Replica) {
		id, err := r.IDAt(0)
		if err != nil {
			t.Fatal(err)
		}
		anchor = id
	})
	if _, err := cs.CreateThread(s.AuthCtx(ctx, "commenter"), &commentv1.CreateThreadRequest{
		DocumentId: docID,
		Anchor:     &commentv1.Anchor{Start: opID(anchor), End: opID(anchor)},
		Author:     "commenter",
		Body:       "note",
		RequestId:  t.Name(),
	}); err != nil {
		t.Fatal(err)
	}
	_, err := cs.CreateThread(s.AuthCtx(ctx, "commenter"), &commentv1.CreateThreadRequest{
		DocumentId: docID,
		Anchor:     &commentv1.Anchor{Start: opID(anchor), End: opID(anchor)},
		Author:     "someoneelse",
		Body:       "nope",
		RequestId:  t.Name() + "-mismatch",
	})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("author mismatch: %v", err)
	}

	commenter := dialUser(t, ctx, s, docID, "commenter", 13)
	if err := commenter.InsertAt(0, 'C'); err != nil {
		t.Fatal(err)
	}
	waitCode(t, commenter, collabv1.ErrorMessage_CODE_FORBIDDEN)
	if editor.Text() != "E" {
		t.Fatalf("editor text %q after commenter batch", editor.Text())
	}
	if err := editor.InsertAt(1, 'F'); err != nil {
		t.Fatal(err)
	}
	waitContains(t, viewer, "F")

	_, err = s.Docs.ArchiveDocument(s.AuthCtx(ctx, "viewer"), &documentv1.ArchiveDocumentRequest{Id: docID})
	if status.Code(err) != codes.PermissionDenied {
		t.Fatalf("viewer archive: %v", err)
	}
}

func TestRevokeAndDowngrade(t *testing.T) {
	s := stack.New(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	docID := s.CreateDocument(ctx)
	s.Grant(ctx, docID, "idle", permissionv1.Role_ROLE_EDITOR)
	s.Grant(ctx, docID, "writer", permissionv1.Role_ROLE_EDITOR)
	idle := dialUser(t, ctx, s, docID, "idle", 21)
	writer := dialUser(t, ctx, s, docID, "writer", 22)
	owner := dialUser(t, ctx, s, docID, s.Owner(), 23)

	if _, err := s.Perm.Client.Revoke(s.AuthCtx(ctx, s.Owner()), &permissionv1.RevokeRequest{DocId: docID, UserId: "idle"}); err != nil {
		t.Fatal(err)
	}
	waitCodeWithin(t, idle, collabv1.ErrorMessage_CODE_REVOKED, 2*time.Second)

	if _, err := s.Perm.Client.Grant(s.AuthCtx(ctx, s.Owner()), &permissionv1.GrantRequest{
		DocId: docID, UserId: "writer", Role: permissionv1.Role_ROLE_VIEWER,
	}); err != nil {
		t.Fatal(err)
	}
	time.Sleep(700 * time.Millisecond)
	if err := writer.InsertAt(0, 'W'); err != nil {
		t.Fatal(err)
	}
	waitCode(t, writer, collabv1.ErrorMessage_CODE_FORBIDDEN)
	if !writer.Connected() {
		t.Fatal("downgraded socket closed")
	}
	if err := owner.InsertAt(0, 'Z'); err != nil {
		t.Fatal(err)
	}
	waitContains(t, writer, "Z")
	if strings.Contains(owner.Text(), "W") {
		t.Fatal("downgraded batch was stored")
	}
}

func TestReplicaBinding(t *testing.T) {
	s := stack.New(t)
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()
	docID := s.CreateDocument(ctx)
	s.Grant(ctx, docID, "ada", permissionv1.Role_ROLE_EDITOR)
	s.Grant(ctx, docID, "bob", permissionv1.Role_ROLE_VIEWER)
	ada := dialUser(t, ctx, s, docID, "ada", 31)

	for i := 0; i < 4096; i++ {
		c, err := collabclient.DialWith(ctx, docID, collabclient.Options{
			URLs:  []string{s.HubURL(0)},
			Token: s.Token(s.Owner()),
		})
		if err != nil {
			t.Fatalf("fill %d: %v", i, err)
		}
		_ = c.Close()
	}

	err := dialHub(ctx, s, docID, s.Token("bob"), ada.ID())
	if err == nil || !strings.Contains(err.Error(), "CODE_REPLICA_FORBIDDEN") {
		t.Fatalf("bob claim of ada replica: %v", err)
	}
	if _, err := s.Perm.Client.Revoke(s.AuthCtx(ctx, s.Owner()), &permissionv1.RevokeRequest{DocId: docID, UserId: "ada"}); err != nil {
		t.Fatal(err)
	}
	waitCodeWithin(t, ada, collabv1.ErrorMessage_CODE_REVOKED, 2*time.Second)
	err = dialHub(ctx, s, docID, s.Token("bob"), ada.ID())
	if err == nil || !strings.Contains(err.Error(), "CODE_REPLICA_FORBIDDEN") {
		t.Fatalf("bob claim after revoke: %v", err)
	}
}

func rawDocs(t *testing.T, addr string) documentv1.DocumentServiceClient {
	t.Helper()
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })
	return documentv1.NewDocumentServiceClient(conn)
}

func bearer(ctx context.Context, token string) context.Context {
	if token == "" {
		return ctx
	}
	return metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+token)
}

func dialHub(ctx context.Context, s *stack.Stack, docID, token string, client crdt.ClientID) error {
	_, err := collabclient.DialWith(ctx, docID, collabclient.Options{
		URLs:   []string{s.HubURL(0)},
		Client: client,
		Token:  token,
	})
	return err
}

func dialUser(t *testing.T, ctx context.Context, s *stack.Stack, docID, user string, client crdt.ClientID) *collabclient.Client {
	t.Helper()
	c, err := collabclient.DialWith(ctx, docID, collabclient.Options{
		URLs:   []string{s.HubURL(0)},
		Client: client,
		Token:  s.Token(user),
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.Close() })
	return c
}

func waitContains(t *testing.T, c *collabclient.Client, part string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if strings.Contains(c.Text(), part) {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("text %q does not contain %q", c.Text(), part)
}

func waitCode(t *testing.T, c *collabclient.Client, code collabv1.ErrorMessage_Code) {
	t.Helper()
	waitCodeWithin(t, c, code, 5*time.Second)
}

func waitCodeWithin(t *testing.T, c *collabclient.Client, code collabv1.ErrorMessage_Code, budget time.Duration) {
	t.Helper()
	deadline := time.Now().Add(budget)
	for time.Now().Before(deadline) {
		for _, e := range c.Errors() {
			if e.GetCode() == code {
				return
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("missing %s within %s; errors %v", code, budget, c.Errors())
}

func flipSig(token string) string {
	rest := strings.TrimPrefix(token, "v1.")
	payload, sig, _ := strings.Cut(rest, ".")
	raw, err := base64.RawURLEncoding.DecodeString(sig)
	if err != nil || len(raw) == 0 {
		return token
	}
	raw[0] ^= 0xff
	return "v1." + payload + "." + base64.RawURLEncoding.EncodeToString(raw)
}

func opID(id crdt.ID) *crdtv1.OpID {
	return &crdtv1.OpID{Client: uint64(id.Client), Clock: id.Clock}
}
