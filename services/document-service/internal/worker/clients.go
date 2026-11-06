package worker

import (
	"context"
	"fmt"

	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	commentv1 "github.com/rowjay007/collab-sphere/gen/comment/v1"
	crdtv1 "github.com/rowjay007/collab-sphere/gen/crdt/v1"
	versionv1 "github.com/rowjay007/collab-sphere/gen/version/v1"
	"github.com/rowjay007/collab-sphere/pkg/auth"
	"github.com/rowjay007/collab-sphere/pkg/crdt"
)

type GRPCClients struct {
	version versionv1.VersionServiceClient
	comment commentv1.CommentServiceClient
}

func Dial(versionAddr, commentAddr, internal string) (*GRPCClients, error) {
	if versionAddr == "" || commentAddr == "" {
		return nil, fmt.Errorf("version and comment addresses are required")
	}
	if _, err := auth.ParseInternal(internal); err != nil {
		return nil, err
	}
	dial := []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithUnaryInterceptor(auth.InternalUnary(internal)),
	}
	vc, err := grpc.NewClient(versionAddr, dial...)
	if err != nil {
		return nil, fmt.Errorf("dial version: %w", err)
	}
	cc, err := grpc.NewClient(commentAddr, dial...)
	if err != nil {
		vc.Close()
		return nil, fmt.Errorf("dial comment: %w", err)
	}
	return &GRPCClients{
		version: versionv1.NewVersionServiceClient(vc),
		comment: commentv1.NewCommentServiceClient(cc),
	}, nil
}

func (c *GRPCClients) Close() error {
	return nil
}

func (c *GRPCClients) RestoreFloor(ctx context.Context, docID uuid.UUID) (crdt.VersionVector, error) {
	resp, err := c.version.RestoreFloor(ctx, &versionv1.RestoreFloorRequest{DocumentId: docID.String()})
	if err != nil {
		return nil, err
	}
	return vectorFromProto(resp.GetVector()), nil
}

func (c *GRPCClients) AnchorPins(ctx context.Context, forkRoot uuid.UUID) (map[crdt.ID]struct{}, error) {
	resp, err := c.comment.AnchorPins(ctx, &commentv1.AnchorPinsRequest{ForkRoot: forkRoot.String()})
	if err != nil {
		return nil, err
	}
	out := make(map[crdt.ID]struct{}, len(resp.GetPins()))
	for _, pin := range resp.GetPins() {
		out[crdt.ID{Client: crdt.ClientID(pin.GetClient()), Clock: pin.GetClock()}] = struct{}{}
	}
	return out, nil
}

func vectorFromProto(entries []*crdtv1.VectorEntry) crdt.VersionVector {
	out := make(crdt.VersionVector, len(entries))
	for _, e := range entries {
		if e.GetClient() == 0 || e.GetClock() == 0 {
			continue
		}
		out[crdt.ClientID(e.GetClient())] = e.GetClock()
	}
	return out
}
