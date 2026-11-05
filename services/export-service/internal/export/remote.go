package export

import (
	"context"
	"strings"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"

	"github.com/rowjay007/collab-sphere/pkg/auth"
)

func DialDocument(addr, internal string) (*grpc.ClientConn, error) {
	return grpc.NewClient(addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithUnaryInterceptor(auth.InternalUnary(internal)),
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(64<<20)),
	)
}

func DialVersion(addr, internal string) (*grpc.ClientConn, error) {
	internalMW := auth.InternalUnary(internal)
	return grpc.NewClient(addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(64<<20)),
		grpc.WithUnaryInterceptor(func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
			if strings.HasSuffix(method, "/GetVersion") {
				return invoker(forwardAuthorization(ctx), method, req, reply, cc, opts...)
			}
			return internalMW(ctx, method, req, reply, cc, invoker, opts...)
		}),
	)
}

func forwardAuthorization(ctx context.Context) context.Context {
	md, _ := metadata.FromIncomingContext(ctx)
	vals := md.Get("authorization")
	if len(vals) == 0 {
		return ctx
	}
	return metadata.NewOutgoingContext(ctx, metadata.Pairs("authorization", vals[0]))
}
