package docs

import (
	"context"

	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	documentv1 "github.com/rowjay007/collab-sphere/gen/document/v1"
	"github.com/rowjay007/collab-sphere/pkg/auth"
)

type Client struct {
	rpc  documentv1.DocumentServiceClient
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
	return &Client{rpc: documentv1.NewDocumentServiceClient(conn), conn: conn}, nil
}

func (c *Client) Close() error { return c.conn.Close() }

func (c *Client) SetEpoch(ctx context.Context, doc uuid.UUID, epoch uint32, state string) error {
	_, err := c.rpc.SetEpoch(ctx, &documentv1.SetEpochRequest{
		Id:         doc.String(),
		WriteEpoch: epoch,
		EpochState: state,
	})
	return err
}
