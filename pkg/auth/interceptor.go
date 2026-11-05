package auth

import (
	"context"
	"crypto/ed25519"
	"strings"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

type Class int

const (
	User Class = iota + 1
	Internal
	Either
)

type identityKey struct{}

func Subject(ctx context.Context) string {
	s, _ := ctx.Value(identityKey{}).(string)
	return s
}

func WithSubject(ctx context.Context, subject string) context.Context {
	return context.WithValue(ctx, identityKey{}, subject)
}

func UnaryInterceptor(pub ed25519.PublicKey, internal string, methods map[string]Class) grpc.UnaryServerInterceptor {
	v := newVerifier(pub, internal, methods)
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		ctx, err := v.authenticate(ctx, info.FullMethod)
		if err != nil {
			return nil, err
		}
		return handler(ctx, req)
	}
}

func StreamInterceptor(pub ed25519.PublicKey, internal string, methods map[string]Class) grpc.StreamServerInterceptor {
	v := newVerifier(pub, internal, methods)
	return func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		ctx, err := v.authenticate(ss.Context(), info.FullMethod)
		if err != nil {
			return err
		}
		return handler(srv, &boundStream{ServerStream: ss, ctx: ctx})
	}
}

type verifier struct {
	pub      ed25519.PublicKey
	internal string
	methods  map[string]Class
}

func newVerifier(pub ed25519.PublicKey, internal string, methods map[string]Class) *verifier {
	secret, _ := ParseInternal(internal)
	return &verifier{pub: pub, internal: secret, methods: methods}
}

func (v *verifier) authenticate(ctx context.Context, method string) (context.Context, error) {
	class, ok := v.methods[method]
	if !ok {
		return nil, status.Error(codes.Unauthenticated, "unauthenticated")
	}
	raw, err := bearer(ctx)
	if err != nil {
		return nil, err
	}
	switch {
	case strings.HasPrefix(raw, tokenPrefix):
		if class == Internal {
			return nil, status.Error(codes.Unauthenticated, "unauthenticated")
		}
		subject, err := Verify(v.pub, raw, time.Now())
		if err != nil {
			return nil, status.Error(codes.Unauthenticated, "unauthenticated")
		}
		return WithSubject(ctx, subject), nil
	case strings.HasPrefix(raw, internalPrefix):
		if class == User {
			return nil, status.Error(codes.Unauthenticated, "unauthenticated")
		}
		if !MatchInternal(raw, v.internal) {
			return nil, status.Error(codes.Unauthenticated, "unauthenticated")
		}
		return ctx, nil
	default:
		return nil, status.Error(codes.Unauthenticated, "unauthenticated")
	}
}

func bearer(ctx context.Context) (string, error) {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return "", status.Error(codes.Unauthenticated, "unauthenticated")
	}
	vals := md.Get("authorization")
	if len(vals) == 0 {
		return "", status.Error(codes.Unauthenticated, "unauthenticated")
	}
	const prefix = "Bearer "
	if !strings.HasPrefix(vals[0], prefix) {
		return "", status.Error(codes.Unauthenticated, "unauthenticated")
	}
	tok := strings.TrimSpace(vals[0][len(prefix):])
	if tok == "" {
		return "", status.Error(codes.Unauthenticated, "unauthenticated")
	}
	return tok, nil
}

type boundStream struct {
	grpc.ServerStream
	ctx context.Context
}

func (s *boundStream) Context() context.Context { return s.ctx }
