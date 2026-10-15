package client

import (
	"context"
	"errors"
	"net/http"
	"strconv"
	"time"

	"github.com/coder/websocket"
	"github.com/google/uuid"
	"google.golang.org/protobuf/proto"

	collabv1 "github.com/rowjay007/collab-sphere/gen/collab/v1"
	crdtv1 "github.com/rowjay007/collab-sphere/gen/crdt/v1"
	"github.com/rowjay007/collab-sphere/pkg/crdt"
	"github.com/rowjay007/collab-sphere/services/collab-hub/internal/hub"
)

const (
	readLimit         = 1 << 20
	maxOpsPerBatch    = 1024
	maxNameBytes      = 64
	writeTimeout      = 10 * time.Second
	drainTimeout      = 100 * time.Millisecond
	keepaliveInterval = 30 * time.Second
)

// Handler serves GET /ws/{docID}?client=<nonzero uint64>[&seq=<uint64>][&conn=<uint64>].
// seq is the last seq a resuming client holds; without it the join is fresh.
// conn is the client's connection count, its presence epoch; absent or
// invalid means zero.
func Handler(reg *hub.Registry) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		documentID, err := uuid.Parse(r.PathValue("docID"))
		if err != nil {
			http.Error(w, "invalid document id", http.StatusBadRequest)
			return
		}
		docID := documentID.String()
		query := r.URL.Query()
		clientID, err := parseClient(query.Get("client"))
		if err != nil {
			http.Error(w, "invalid client", http.StatusBadRequest)
			return
		}
		resume := query.Has("seq")
		var seq uint64
		if resume {
			seq, err = strconv.ParseUint(query.Get("seq"), 10, 64)
			if err != nil {
				http.Error(w, "invalid seq", http.StatusBadRequest)
				return
			}
		}

		conn, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		conn.SetReadLimit(readLimit)
		defer conn.CloseNow()

		epoch, _ := strconv.ParseUint(query.Get("conn"), 10, 64)
		peer := hub.NewPeer(clientID, epoch)
		ctx, cancel := context.WithCancel(r.Context())
		defer cancel()

		writeDone := make(chan struct{})
		go func() {
			writePump(ctx, conn, peer)
			close(writeDone)
			cancel()
		}()

		session, err := reg.Join(ctx, docID, peer, resume, seq)
		if err != nil {
			if errors.Is(err, hub.ErrStopped) {
				peer.TrySend(hub.EncodeError(collabv1.ErrorMessage_CODE_UNAVAILABLE, "hub stopped"))
			}
			peer.Close()
			<-writeDone
			return
		}

		readPump(ctx, conn, session, peer)
		session.Leave(context.Background())
		peer.Close()
		<-writeDone
	})
}

func parseClient(s string) (crdt.ClientID, error) {
	n, err := strconv.ParseUint(s, 10, 64)
	if err != nil || n == 0 {
		return 0, strconv.ErrSyntax
	}
	return crdt.ClientID(n), nil
}

func writePump(ctx context.Context, conn *websocket.Conn, peer *hub.Peer) {
	ticker := time.NewTicker(keepaliveInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			drainWrites(conn, peer)
			return
		case <-peer.Done():
			drainWrites(conn, peer)
			_ = conn.Close(websocket.StatusPolicyViolation, "dropped: "+peer.DropReason())
			return
		case frame := <-peer.Frames():
			if err := writeFrame(ctx, conn, frame); err != nil {
				_ = conn.CloseNow()
				return
			}
		case <-ticker.C:
			pingCtx, cancel := context.WithTimeout(ctx, writeTimeout)
			err := conn.Ping(pingCtx)
			cancel()
			if err != nil {
				_ = conn.CloseNow()
				return
			}
		}
	}
}

func drainWrites(conn *websocket.Conn, peer *hub.Peer) {
	ctx, cancel := context.WithTimeout(context.Background(), drainTimeout)
	defer cancel()
	for {
		select {
		case frame := <-peer.Frames():
			if writeFrame(ctx, conn, frame) != nil {
				return
			}
		default:
			return
		}
	}
}

func writeFrame(ctx context.Context, conn *websocket.Conn, frame []byte) error {
	writeCtx, cancel := context.WithTimeout(ctx, writeTimeout)
	defer cancel()
	return conn.Write(writeCtx, websocket.MessageBinary, frame)
}

func readPump(ctx context.Context, conn *websocket.Conn, session *hub.Session, peer *hub.Peer) {
	for {
		typ, data, err := conn.Read(ctx)
		if err != nil {
			return
		}
		if typ != websocket.MessageBinary {
			_ = conn.Close(websocket.StatusPolicyViolation, "text frames are not allowed")
			return
		}
		msg := &collabv1.ClientMessage{}
		if err := proto.Unmarshal(data, msg); err != nil {
			peer.TrySend(hub.EncodeError(collabv1.ErrorMessage_CODE_MALFORMED_MESSAGE, "malformed message"))
			return
		}
		if !handleClientMessage(ctx, session, peer, msg) {
			return
		}
	}
}

func handleClientMessage(ctx context.Context, session *hub.Session, peer *hub.Peer, msg *collabv1.ClientMessage) bool {
	switch p := msg.GetPayload().(type) {
	case *collabv1.ClientMessage_OperationBatch:
		ops, ok := parseBatch(peer, p.OperationBatch)
		if !ok {
			return false
		}
		if len(ops) > 0 {
			session.SubmitOps(ctx, ops)
		}
		return true
	case *collabv1.ClientMessage_CursorMove:
		session.MoveCursor(ctx, idFromProto(p.CursorMove.GetAnchor()), idFromProto(p.CursorMove.GetHead()))
		return true
	case *collabv1.ClientMessage_AwarenessUpdate:
		name := p.AwarenessUpdate.GetDisplayName()
		if len(name) > maxNameBytes {
			peer.TrySend(hub.EncodeError(collabv1.ErrorMessage_CODE_MALFORMED_MESSAGE, "display name too long"))
			return false
		}
		session.Rename(ctx, name)
		return true
	case *collabv1.ClientMessage_Ping:
		if !peer.TrySend(hub.EncodePong(p.Ping.GetNonce())) {
			return false
		}
		return true
	default:
		peer.TrySend(hub.EncodeError(collabv1.ErrorMessage_CODE_MALFORMED_MESSAGE, "malformed message"))
		return false
	}
}

func parseBatch(peer *hub.Peer, batch *collabv1.OperationBatch) ([]crdt.Op, bool) {
	if batch == nil {
		peer.TrySend(hub.EncodeError(collabv1.ErrorMessage_CODE_MALFORMED_MESSAGE, "malformed message"))
		return nil, false
	}
	raw := batch.GetOperations()
	if len(raw) > maxOpsPerBatch {
		peer.TrySend(hub.EncodeError(collabv1.ErrorMessage_CODE_MALFORMED_MESSAGE, "malformed message"))
		return nil, false
	}
	ops := make([]crdt.Op, 0, len(raw))
	for _, pb := range raw {
		op, err := crdt.OpFromProto(pb)
		if err != nil {
			peer.TrySend(hub.EncodeError(collabv1.ErrorMessage_CODE_MALFORMED_MESSAGE, "malformed message"))
			return nil, false
		}
		ops = append(ops, op)
	}
	return ops, true
}

func idFromProto(id *crdtv1.OpID) crdt.ID {
	if id == nil {
		return crdt.Root
	}
	return crdt.ID{Client: crdt.ClientID(id.GetClient()), Clock: id.GetClock()}
}
