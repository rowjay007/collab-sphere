package e2e

import (
	"context"
	"encoding/base64"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	collabv1 "github.com/rowjay007/collab-sphere/gen/collab/v1"
	commentv1 "github.com/rowjay007/collab-sphere/gen/comment/v1"
	permissionv1 "github.com/rowjay007/collab-sphere/gen/permission/v1"
	"github.com/rowjay007/collab-sphere/pkg/crdt"
	"github.com/rowjay007/collab-sphere/tests/internal/stack"
)

func TestShareLinks(t *testing.T) {
	s := stack.New(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	vs := stack.StartVersionService(s, 0)
	cs := stack.StartCommentService(s, vs)
	docID := s.CreateDocument(ctx)
	otherDoc := uuid.NewString()

	commenterLink := createLink(t, ctx, s, docID, permissionv1.Role_ROLE_COMMENTER, time.Now().Add(time.Hour))

	for _, tc := range []struct {
		name   string
		token  string
		reason string
	}{
		{"payload byte", flipLinkPayload(commenterLink.GetToken()), "LINK_BAD"},
		{"signature byte", flipLinkSignature(commenterLink.GetToken()), "LINK_BAD"},
		{"swapped doc", s.Perm.SwapLinkDoc(commenterLink.GetToken(), otherDoc), "LINK_BAD"},
		{"expired store row", s.Perm.ExpiredShareToken(ctx, docID, s.Owner()), "LINK_EXPIRED"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := s.Perm.Client.Redeem(s.AuthCtx(ctx, "pat"), &permissionv1.RedeemRequest{Token: tc.token})
			if status.Code(err) != codes.FailedPrecondition || linkReason(err) != tc.reason {
				t.Fatalf("redeem: %v code %s reason %q want %s %s", err, status.Code(err), linkReason(err), codes.FailedPrecondition, tc.reason)
			}
		})
	}

	revoked := createLink(t, ctx, s, docID, permissionv1.Role_ROLE_VIEWER, time.Now().Add(time.Hour))
	if _, err := s.Perm.Client.RevokeLink(s.AuthCtx(ctx, s.Owner()), &permissionv1.RevokeLinkRequest{Id: revoked.GetId()}); err != nil {
		t.Fatal(err)
	}
	_, err := s.Perm.Client.Redeem(s.AuthCtx(ctx, "pat"), &permissionv1.RedeemRequest{Token: revoked.GetToken()})
	if status.Code(err) != codes.FailedPrecondition || linkReason(err) != "LINK_REVOKED" {
		t.Fatalf("revoked redeem: %v reason %q", err, linkReason(err))
	}

	_, err = s.Perm.Client.CreateLink(s.AuthCtx(ctx, "zoe"), &permissionv1.CreateLinkRequest{
		DocId:     docID,
		Role:      permissionv1.Role_ROLE_VIEWER,
		ExpiresAt: timestamppb.New(time.Now().Add(time.Hour)),
	})
	if status.Code(err) != codes.PermissionDenied {
		t.Fatalf("non-owner create: %v", err)
	}
	s.Grant(ctx, docID, "ed", permissionv1.Role_ROLE_EDITOR)
	_, err = s.Perm.Client.CreateLink(s.AuthCtx(ctx, "ed"), &permissionv1.CreateLinkRequest{
		DocId:     docID,
		Role:      permissionv1.Role_ROLE_EDITOR,
		ExpiresAt: timestamppb.New(time.Now().Add(time.Hour)),
	})
	if status.Code(err) != codes.PermissionDenied {
		t.Fatalf("editor create editor link: %v (owner-only, design table)", err)
	}

	viewerLink := createLink(t, ctx, s, docID, permissionv1.Role_ROLE_VIEWER, time.Now().Add(time.Hour))
	kept, err := s.Perm.Client.Redeem(s.AuthCtx(ctx, s.Owner()), &permissionv1.RedeemRequest{Token: viewerLink.GetToken()})
	if err != nil || kept.GetRole() != permissionv1.Role_ROLE_OWNER {
		t.Fatalf("owner redeem viewer: %+v %v", kept, err)
	}
	s.Grant(ctx, docID, "keep", permissionv1.Role_ROLE_VIEWER)

	granted, err := s.Perm.Client.Redeem(s.AuthCtx(ctx, "pat"), &permissionv1.RedeemRequest{Token: commenterLink.GetToken()})
	if err != nil || granted.GetRole() != permissionv1.Role_ROLE_COMMENTER {
		t.Fatalf("clean redeem: %+v %v", granted, err)
	}

	owner := dialUser(t, ctx, s, docID, s.Owner(), 51)
	if err := owner.InsertAt(0, 'A'); err != nil {
		t.Fatal(err)
	}
	pat := dialUser(t, ctx, s, docID, "pat", 52)
	waitContains(t, pat, "A")
	var anchor crdt.ID
	owner.Read(func(r *crdt.Replica) {
		id, err := r.IDAt(0)
		if err != nil {
			t.Fatal(err)
		}
		anchor = id
	})
	if _, err := cs.CreateThread(s.AuthCtx(ctx, "pat"), &commentv1.CreateThreadRequest{
		DocumentId: docID,
		Anchor:     &commentv1.Anchor{Start: opID(anchor), End: opID(anchor)},
		Author:     "pat",
		Body:       "from-link",
		RequestId:  t.Name() + "-first",
	}); err != nil {
		t.Fatal(err)
	}

	if _, err := s.Perm.Client.RevokeLink(s.AuthCtx(ctx, s.Owner()), &permissionv1.RevokeLinkRequest{Id: commenterLink.GetId()}); err != nil {
		t.Fatal(err)
	}
	if _, err := cs.CreateThread(s.AuthCtx(ctx, "pat"), &commentv1.CreateThreadRequest{
		DocumentId: docID,
		Anchor:     &commentv1.Anchor{Start: opID(anchor), End: opID(anchor)},
		Author:     "pat",
		Body:       "after-revoke",
		RequestId:  t.Name() + "-kept",
	}); err != nil {
		t.Fatal(err)
	}

	ed := dialUser(t, ctx, s, docID, "ed", 53)
	if err := pat.InsertAt(1, 'X'); err != nil {
		t.Fatal(err)
	}
	waitCode(t, pat, collabv1.ErrorMessage_CODE_FORBIDDEN)
	if !pat.Connected() {
		t.Fatal("commenter socket closed after CODE_FORBIDDEN")
	}
	if err := ed.InsertAt(1, 'E'); err != nil {
		t.Fatal(err)
	}
	waitContains(t, pat, "E")
	if strings.Contains(ed.Text(), "X") || strings.Contains(owner.Text(), "X") {
		t.Fatal("commenter batch was stored")
	}
}

func createLink(t *testing.T, ctx context.Context, s *stack.Stack, docID string, role permissionv1.Role, exp time.Time) *permissionv1.ShareLink {
	t.Helper()
	link, err := s.Perm.Client.CreateLink(s.AuthCtx(ctx, s.Owner()), &permissionv1.CreateLinkRequest{
		DocId:     docID,
		Role:      role,
		ExpiresAt: timestamppb.New(exp),
	})
	if err != nil {
		t.Fatal(err)
	}
	if link.GetToken() == "" {
		t.Fatal("empty link token")
	}
	return link
}

func flipLinkPayload(token string) string {
	return rewriteLink(token, func(payload, sig []byte) ([]byte, []byte) {
		payload[0] ^= 0xff
		return payload, sig
	})
}

func flipLinkSignature(token string) string {
	return rewriteLink(token, func(payload, sig []byte) ([]byte, []byte) {
		sig[0] ^= 0xff
		return payload, sig
	})
}

func rewriteLink(token string, fn func(payload, sig []byte) ([]byte, []byte)) string {
	rest := strings.TrimPrefix(token, "s1.")
	payloadB64, sigB64, _ := strings.Cut(rest, ".")
	payload, _ := base64.RawURLEncoding.DecodeString(payloadB64)
	sig, _ := base64.RawURLEncoding.DecodeString(sigB64)
	payload, sig = fn(payload, sig)
	return "s1." + base64.RawURLEncoding.EncodeToString(payload) + "." + base64.RawURLEncoding.EncodeToString(sig)
}

func linkReason(err error) string {
	st, ok := status.FromError(err)
	if !ok {
		return ""
	}
	for _, d := range st.Details() {
		if info, ok := d.(*errdetails.ErrorInfo); ok {
			return info.GetReason()
		}
	}
	return ""
}
