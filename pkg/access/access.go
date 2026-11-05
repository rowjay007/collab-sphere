package access

import (
	"context"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	permissionv1 "github.com/rowjay007/collab-sphere/gen/permission/v1"
	"github.com/rowjay007/collab-sphere/pkg/auth"
)

// Client calls permission-service with the internal token.
type Client struct {
	rpc  permissionv1.PermissionServiceClient
	conn *grpc.ClientConn
}

func Dial(addr, internal string) (*Client, error) {
	conn, err := grpc.NewClient(addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithUnaryInterceptor(auth.InternalUnary(internal)),
	)
	if err != nil {
		return nil, err
	}
	return &Client{rpc: permissionv1.NewPermissionServiceClient(conn), conn: conn}, nil
}

func (c *Client) Close() error {
	if c == nil || c.conn == nil {
		return nil
	}
	return c.conn.Close()
}

func Rank(r permissionv1.Role) int {
	switch r {
	case permissionv1.Role_ROLE_OWNER:
		return 4
	case permissionv1.Role_ROLE_EDITOR:
		return 3
	case permissionv1.Role_ROLE_COMMENTER:
		return 2
	case permissionv1.Role_ROLE_VIEWER:
		return 1
	default:
		return 0
	}
}

func (c *Client) EnsureOwner(ctx context.Context, docID, userID string) error {
	_, err := c.rpc.EnsureOwner(ctx, &permissionv1.EnsureOwnerRequest{DocId: docID, UserId: userID})
	return err
}

func (c *Client) CopyGrants(ctx context.Context, src, dst string) error {
	_, err := c.rpc.CopyGrants(ctx, &permissionv1.CopyGrantsRequest{SourceDocId: src, DestDocId: dst})
	return err
}

func (c *Client) Dump(ctx context.Context, docID string) (*permissionv1.PolicyDumpResponse, error) {
	return c.rpc.PolicyDump(ctx, &permissionv1.PolicyDumpRequest{DocId: docID})
}

// Require allows user when their grant is at least min.
func (c *Client) Require(ctx context.Context, docID, user string, min permissionv1.Role) error {
	resp, err := c.Dump(ctx, docID)
	if err != nil {
		return err
	}
	if Rank(resp.GetMembers()[user]) < Rank(min) {
		return status.Error(codes.PermissionDenied, "permission denied")
	}
	return nil
}
