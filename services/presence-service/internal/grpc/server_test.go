package presgrpc_test

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	crdtv1 "github.com/rowjay007/collab-sphere/gen/crdt/v1"
	presencev1 "github.com/rowjay007/collab-sphere/gen/presence/v1"
	presgrpc "github.com/rowjay007/collab-sphere/services/presence-service/internal/grpc"
	"github.com/rowjay007/collab-sphere/services/presence-service/internal/store"
	"github.com/rowjay007/collab-sphere/services/presence-service/internal/testredis"
)

func TestGRPCJoinApplyList(t *testing.T) {
	client, _ := startGRPC(t, time.Second)
	ctx := t.Context()
	doc := uuid.NewString()

	joined, err := client.Join(ctx, &presencev1.JoinRequest{Self: &presencev1.State{
		DocumentId:  doc,
		Client:      1,
		Session:     10,
		Epoch:       1,
		DisplayName: "ann",
		HasCursor:   true,
		Anchor:      &crdtv1.OpID{Client: 1, Clock: 3},
		Head:        &crdtv1.OpID{Client: 1, Clock: 3},
	}})
	if err != nil {
		t.Fatalf("join: %v", err)
	}
	if joined.GetVersion() == 0 || len(joined.GetStates()) != 0 {
		t.Fatalf("join self %+v", joined)
	}

	peer, err := client.Join(ctx, &presencev1.JoinRequest{Self: &presencev1.State{
		DocumentId:  doc,
		Client:      2,
		Session:     20,
		Epoch:       1,
		DisplayName: "bob",
	}})
	if err != nil {
		t.Fatal(err)
	}
	if peer.GetVersion() <= joined.GetVersion() || len(peer.GetStates()) != 1 || peer.GetStates()[0].GetClient() != 1 {
		t.Fatalf("join others %+v", peer)
	}
	if peer.GetStates()[0].GetHasCursor() {
		t.Fatalf("join kept cursor %+v", peer.GetStates()[0])
	}

	_, err = client.Apply(ctx, &presencev1.ApplyRequest{Changes: []*presencev1.Change{{
		Kind: &presencev1.Change_Upsert{Upsert: &presencev1.State{
			DocumentId:  doc,
			Client:      1,
			Session:     10,
			DisplayName: "ann",
			HasCursor:   true,
			Anchor:      &crdtv1.OpID{Client: 1, Clock: 8},
			Head:        &crdtv1.OpID{Client: 1, Clock: 9},
		}},
	}}})
	if err != nil {
		t.Fatalf("upsert: %v", err)
	}

	listed, err := client.List(ctx, &presencev1.ListRequest{DocumentId: doc})
	if err != nil {
		t.Fatal(err)
	}
	if listed.GetVersion() <= peer.GetVersion() || len(listed.GetStates()) != 2 {
		t.Fatalf("list %+v", listed)
	}

	touched, err := client.Apply(ctx, &presencev1.ApplyRequest{Changes: []*presencev1.Change{
		{Kind: &presencev1.Change_Upsert{Upsert: &presencev1.State{DocumentId: doc, Client: 1, Session: 99}}},
		{Kind: &presencev1.Change_Touch{Touch: &presencev1.Touch{DocumentId: doc, Client: 1, Session: 10}}},
		{Kind: &presencev1.Change_Touch{Touch: &presencev1.Touch{DocumentId: doc, Client: 1, Session: 99}}},
		{Kind: &presencev1.Change_Touch{Touch: &presencev1.Touch{DocumentId: doc, Client: 3, Session: 30}}},
		{Kind: &presencev1.Change_Leave{Leave: &presencev1.Leave{DocumentId: doc, Client: 2, Session: 20}}},
	}})
	if err != nil {
		t.Fatalf("apply: %v", err)
	}
	if len(touched.GetMissing()) != 1 || touched.GetMissing()[0].GetSession() != 30 {
		t.Fatalf("missing %+v", touched.GetMissing())
	}
	if len(touched.GetSuperseded()) != 1 || touched.GetSuperseded()[0].GetSession() != 99 {
		t.Fatalf("superseded %+v", touched.GetSuperseded())
	}
	if len(touched.GetSupersededUpserts()) != 1 || touched.GetSupersededUpserts()[0].GetSession() != 99 {
		t.Fatalf("superseded upserts %+v", touched.GetSupersededUpserts())
	}

	listed, err = client.List(ctx, &presencev1.ListRequest{DocumentId: doc})
	if err != nil {
		t.Fatal(err)
	}
	if listed.GetVersion() <= peer.GetVersion() || len(listed.GetStates()) != 1 || listed.GetStates()[0].GetClient() != 1 {
		t.Fatalf("after leave %+v", listed)
	}
}

func TestGRPCJoinReportsSupersededEpoch(t *testing.T) {
	client, _ := startGRPC(t, time.Second)
	ctx := t.Context()
	doc := uuid.NewString()

	if _, err := client.Join(ctx, &presencev1.JoinRequest{Self: &presencev1.State{
		DocumentId: doc,
		Client:     1,
		Session:    20,
		Epoch:      2,
	}}); err != nil {
		t.Fatal(err)
	}
	late, err := client.Join(ctx, &presencev1.JoinRequest{Self: &presencev1.State{
		DocumentId: doc,
		Client:     1,
		Session:    10,
		Epoch:      1,
	}})
	if err != nil {
		t.Fatal(err)
	}
	if !late.GetSuperseded() {
		t.Fatalf("join response %+v", late)
	}
	listed, err := client.List(ctx, &presencev1.ListRequest{DocumentId: doc})
	if err != nil {
		t.Fatal(err)
	}
	if len(listed.GetStates()) != 1 || listed.GetStates()[0].GetSession() != 20 {
		t.Fatalf("list %+v", listed)
	}
}

func TestGRPCValidation(t *testing.T) {
	client, _ := startGRPC(t, time.Second)
	ctx := t.Context()
	doc := uuid.NewString()

	_, err := client.Join(ctx, &presencev1.JoinRequest{})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("nil self: %v", err)
	}
	_, err = client.Join(ctx, &presencev1.JoinRequest{Self: &presencev1.State{DocumentId: "nope", Client: 1, Session: 1}})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("bad uuid: %v", err)
	}
	_, err = client.Join(ctx, &presencev1.JoinRequest{Self: &presencev1.State{DocumentId: doc, Client: 0, Session: 1}})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("zero client: %v", err)
	}
	_, err = client.Join(ctx, &presencev1.JoinRequest{Self: &presencev1.State{DocumentId: doc, Client: 1, Session: 0}})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("zero session: %v", err)
	}
	_, err = client.List(ctx, &presencev1.ListRequest{DocumentId: "nope"})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("list uuid: %v", err)
	}
	_, err = client.Apply(ctx, &presencev1.ApplyRequest{Changes: []*presencev1.Change{{}}})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("empty change: %v", err)
	}
	_, err = client.Apply(ctx, &presencev1.ApplyRequest{Changes: []*presencev1.Change{{
		Kind: &presencev1.Change_Touch{Touch: &presencev1.Touch{DocumentId: doc, Client: 0, Session: 1}},
	}}})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("touch zero client: %v", err)
	}
}

func TestGRPCApplyValidatesBatchBeforeWriting(t *testing.T) {
	client, _ := startGRPC(t, time.Second)
	ctx := t.Context()
	doc := uuid.NewString()

	_, err := client.Apply(ctx, &presencev1.ApplyRequest{Changes: []*presencev1.Change{
		{Kind: &presencev1.Change_Upsert{Upsert: &presencev1.State{
			DocumentId: doc,
			Client:     1,
			Session:    1,
		}}},
		{},
	}})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("apply: %v", err)
	}

	listed, err := client.List(ctx, &presencev1.ListRequest{DocumentId: doc})
	if err != nil {
		t.Fatal(err)
	}
	if len(listed.GetStates()) != 0 || listed.GetVersion() != 0 {
		t.Fatalf("partial write %+v", listed)
	}
}

func startGRPC(t *testing.T, ttl time.Duration) (presencev1.PresenceServiceClient, func()) {
	t.Helper()
	rdb := testredis.Client(t)
	lis := bufconn.Listen(1024 * 1024)
	server := grpc.NewServer()
	presencev1.RegisterPresenceServiceServer(server, presgrpc.New(store.New(rdb, ttl)))
	go server.Serve(lis)

	conn, err := grpc.NewClient("passthrough://bufnet",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return lis.Dial()
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	stop := func() {
		_ = conn.Close()
		server.Stop()
		_ = lis.Close()
	}
	t.Cleanup(stop)
	return presencev1.NewPresenceServiceClient(conn), stop
}
