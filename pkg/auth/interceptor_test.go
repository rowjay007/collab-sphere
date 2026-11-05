package auth_test

import (
	"context"
	"crypto/ed25519"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/rowjay007/collab-sphere/pkg/auth"
)

const (
	userMethod     = "/test.Svc/User"
	internalMethod = "/test.Svc/Internal"
	eitherMethod   = "/test.Svc/Either"
	unknownMethod  = "/test.Svc/Unknown"
)

func TestUnaryInterceptorTable(t *testing.T) {
	pub, priv, secret, unary, stream := interceptors(t)
	now := time.Now()
	userTok, err := auth.Mint(priv, "grace", now, now.Add(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	internalTok := auth.FormatInternal(secret)

	type tc struct {
		method   string
		token    string
		want     codes.Code
		sub      string
		internal bool
	}
	cases := []tc{
		{userMethod, userTok, codes.OK, "grace", false},
		{userMethod, internalTok, codes.Unauthenticated, "", false},
		{userMethod, "", codes.Unauthenticated, "", false},
		{internalMethod, internalTok, codes.OK, "", true},
		{internalMethod, userTok, codes.Unauthenticated, "", false},
		{eitherMethod, userTok, codes.OK, "grace", false},
		{eitherMethod, internalTok, codes.OK, "", true},
		{unknownMethod, userTok, codes.Unauthenticated, "", false},
		{unknownMethod, internalTok, codes.Unauthenticated, "", false},
		{userMethod, "s1." + strings.TrimPrefix(userTok, "v1."), codes.Unauthenticated, "", false},
	}
	for _, c := range cases {
		ctx := bearerCtx(c.token)
		var gotSub string
		var gotInternal bool
		_, err := unary(ctx, nil, &grpc.UnaryServerInfo{FullMethod: c.method}, func(ctx context.Context, _ any) (any, error) {
			gotSub = auth.Subject(ctx)
			gotInternal = auth.IsInternal(ctx)
			return "ok", nil
		})
		if status.Code(err) != c.want {
			t.Fatalf("%s token=%q: %v", c.method, c.token, err)
		}
		if err == nil && gotSub != c.sub {
			t.Fatalf("%s subject %q want %q", c.method, gotSub, c.sub)
		}
		if err == nil && gotInternal != c.internal {
			t.Fatalf("%s internal=%v want %v", c.method, gotInternal, c.internal)
		}
	}

	ctx := bearerCtx(userTok)
	err = stream(nil, &stubStream{ctx: ctx}, &grpc.StreamServerInfo{FullMethod: userMethod}, func(_ any, ss grpc.ServerStream) error {
		if auth.Subject(ss.Context()) != "grace" {
			t.Fatalf("stream subject %q", auth.Subject(ss.Context()))
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	err = stream(nil, &stubStream{ctx: bearerCtx(internalTok)}, &grpc.StreamServerInfo{FullMethod: userMethod}, func(_ any, _ grpc.ServerStream) error {
		return nil
	})
	if status.Code(err) != codes.Unauthenticated {
		t.Fatalf("stream internal on user: %v", err)
	}
	_ = pub
}

func TestWithInternal(t *testing.T) {
	ctx := context.Background()
	if auth.IsInternal(ctx) || auth.Subject(ctx) != "" {
		t.Fatal("bare context")
	}
	ctx = auth.WithInternal(ctx)
	if !auth.IsInternal(ctx) {
		t.Fatal("WithInternal")
	}
	if auth.Subject(ctx) != "" {
		t.Fatalf("subject %q", auth.Subject(ctx))
	}
	ctx = auth.WithSubject(ctx, "ada")
	if !auth.IsInternal(ctx) || auth.Subject(ctx) != "ada" {
		t.Fatal("keys should be independent")
	}
}

func interceptors(t *testing.T) (ed25519.PublicKey, ed25519.PrivateKey, string, grpc.UnaryServerInterceptor, grpc.StreamServerInterceptor) {
	t.Helper()
	pub, priv, err := auth.GenerateKeypair()
	if err != nil {
		t.Fatal(err)
	}
	secret := strings.Repeat("ef", 32)
	table := map[string]auth.Class{
		userMethod:     auth.User,
		internalMethod: auth.Internal,
		eitherMethod:   auth.Either,
	}
	return pub, priv, secret, auth.UnaryInterceptor(pub, secret, table), auth.StreamInterceptor(pub, secret, table)
}

func bearerCtx(token string) context.Context {
	if token == "" {
		return context.Background()
	}
	md := metadata.Pairs("authorization", "Bearer "+token)
	return metadata.NewIncomingContext(context.Background(), md)
}

type stubStream struct {
	grpc.ServerStream
	ctx context.Context
}

func (s *stubStream) Context() context.Context { return s.ctx }
