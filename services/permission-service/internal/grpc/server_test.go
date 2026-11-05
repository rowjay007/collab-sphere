package permgrpc_test

import (
	"context"
	"crypto/ed25519"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/types/known/timestamppb"

	permissionv1 "github.com/rowjay007/collab-sphere/gen/permission/v1"
	"github.com/rowjay007/collab-sphere/pkg/auth"
	permgrpc "github.com/rowjay007/collab-sphere/services/permission-service/internal/grpc"
	"github.com/rowjay007/collab-sphere/services/permission-service/internal/perms"
	"github.com/rowjay007/collab-sphere/services/permission-service/internal/perms/testdb"
)

func TestGRPCThroughInterceptor(t *testing.T) {
	client, keys := start(t)
	doc := uuid.NewString()

	_, err := client.PolicyDump(userCtx(t, keys, "alice"), &permissionv1.PolicyDumpRequest{DocId: doc})
	if status.Code(err) != codes.Unauthenticated {
		t.Fatalf("user PolicyDump: %v", err)
	}

	_, err = client.EnsureOwner(userCtx(t, keys, "alice"), &permissionv1.EnsureOwnerRequest{DocId: doc, UserId: "alice"})
	if status.Code(err) != codes.Unauthenticated {
		t.Fatalf("user EnsureOwner: %v", err)
	}

	dump, err := client.PolicyDump(internalCtx(keys), &permissionv1.PolicyDumpRequest{DocId: doc})
	if err != nil || len(dump.GetMembers()) != 0 || dump.GetWriteEpoch() != 0 || dump.GetEpochState() != "" {
		t.Fatalf("empty dump %+v %v", dump, err)
	}
	if _, err := client.EnsureOwner(internalCtx(keys), &permissionv1.EnsureOwnerRequest{DocId: doc, UserId: "alice"}); err != nil {
		t.Fatal(err)
	}

	_, err = client.Grant(internalCtx(keys), &permissionv1.GrantRequest{
		DocId: doc, UserId: "bob", Role: permissionv1.Role_ROLE_EDITOR,
	})
	if status.Code(err) != codes.Unauthenticated {
		t.Fatalf("internal Grant: %v", err)
	}

	g, err := client.Grant(userCtx(t, keys, "alice"), &permissionv1.GrantRequest{
		DocId: doc, UserId: "bob", Role: permissionv1.Role_ROLE_EDITOR,
	})
	if err != nil || g.GetUserId() != "bob" || g.GetRole() != permissionv1.Role_ROLE_EDITOR {
		t.Fatalf("grant %+v %v", g, err)
	}

	expired := mint(t, keys.priv, "alice", time.Now().Add(-2*time.Hour), time.Now().Add(-time.Hour))
	_, err = client.Grant(bearer(expired), &permissionv1.GrantRequest{
		DocId: doc, UserId: "carol", Role: permissionv1.Role_ROLE_VIEWER,
	})
	if status.Code(err) != codes.Unauthenticated {
		t.Fatalf("expired: %v", err)
	}

	link, err := client.CreateLink(userCtx(t, keys, "alice"), &permissionv1.CreateLinkRequest{
		DocId:     doc,
		Role:      permissionv1.Role_ROLE_VIEWER,
		ExpiresAt: timestamppb.New(time.Now().Add(time.Hour)),
	})
	if err != nil {
		t.Fatal(err)
	}
	redeemed, err := client.Redeem(userCtx(t, keys, "alice"), &permissionv1.RedeemRequest{Token: link.GetToken()})
	if err != nil || redeemed.GetRole() != permissionv1.Role_ROLE_OWNER {
		t.Fatalf("never demote %+v %v", redeemed, err)
	}

	_, err = client.Redeem(userCtx(t, keys, "dave"), &permissionv1.RedeemRequest{Token: link.GetToken() + "z"})
	if status.Code(err) != codes.FailedPrecondition || reasonOf(err) != perms.ReasonLinkBad {
		t.Fatalf("bad link: %v reason %s", err, reasonOf(err))
	}

	if _, err := client.RevokeLink(userCtx(t, keys, "alice"), &permissionv1.RevokeLinkRequest{Id: link.GetId()}); err != nil {
		t.Fatal(err)
	}
	_, err = client.Redeem(userCtx(t, keys, "erin"), &permissionv1.RedeemRequest{Token: link.GetToken()})
	if status.Code(err) != codes.FailedPrecondition || reasonOf(err) != perms.ReasonLinkRevoked {
		t.Fatalf("revoked: %v reason %s", err, reasonOf(err))
	}

	_, err = client.Revoke(userCtx(t, keys, "alice"), &permissionv1.RevokeRequest{DocId: doc, UserId: "alice"})
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("revoke owner: %v", err)
	}

	dump, err = client.PolicyDump(internalCtx(keys), &permissionv1.PolicyDumpRequest{DocId: doc})
	if err != nil || dump.GetMembers()["alice"] != permissionv1.Role_ROLE_OWNER || dump.GetMembers()["bob"] != permissionv1.Role_ROLE_EDITOR {
		t.Fatalf("dump %+v %v", dump, err)
	}
}

type keys struct {
	pub      ed25519.PublicKey
	priv     ed25519.PrivateKey
	internal string
}

func start(t *testing.T) (permissionv1.PermissionServiceClient, keys) {
	t.Helper()
	pool, share := testdb.Open(t)
	pub, priv, err := auth.GenerateKeypair()
	if err != nil {
		t.Fatal(err)
	}
	secret := strings.Repeat("ab", 32)
	k := keys{pub: pub, priv: priv, internal: secret}
	lis := bufconn.Listen(1024 * 1024)
	server := grpc.NewServer(
		grpc.UnaryInterceptor(auth.UnaryInterceptor(pub, secret, permgrpc.Methods())),
		grpc.StreamInterceptor(auth.StreamInterceptor(pub, secret, permgrpc.Methods())),
	)
	permissionv1.RegisterPermissionServiceServer(server, permgrpc.New(perms.New(pool, share)))
	go server.Serve(lis)
	t.Cleanup(func() {
		server.Stop()
		_ = lis.Close()
	})
	conn, err := grpc.NewClient("passthrough://bufnet",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return lis.Dial() }),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })
	return permissionv1.NewPermissionServiceClient(conn), k
}

func userCtx(t *testing.T, k keys, subject string) context.Context {
	t.Helper()
	now := time.Now()
	return bearer(mint(t, k.priv, subject, now, now.Add(time.Hour)))
}

func internalCtx(k keys) context.Context {
	return bearer(auth.FormatInternal(k.internal))
}

func mint(t *testing.T, priv ed25519.PrivateKey, subject string, iat, exp time.Time) string {
	t.Helper()
	tok, err := auth.Mint(priv, subject, iat, exp)
	if err != nil {
		t.Fatal(err)
	}
	return tok
}

func bearer(token string) context.Context {
	return metadata.NewOutgoingContext(context.Background(), metadata.Pairs("authorization", "Bearer "+token))
}

func reasonOf(err error) string {
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
