package collabclient

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"
	"google.golang.org/protobuf/proto"

	collabv1 "github.com/rowjay007/collab-sphere/gen/collab/v1"
	crdtv1 "github.com/rowjay007/collab-sphere/gen/crdt/v1"
)

type pipeHub struct {
	t     *testing.T
	conns chan *websocket.Conn
	hold  chan struct{}
	url   string
}

func startPipeHub(t *testing.T) *pipeHub {
	t.Helper()
	h := &pipeHub{t: t, conns: make(chan *websocket.Conn, 4), hold: make(chan struct{})}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		h.conns <- conn
		<-h.hold
		conn.CloseNow()
	}))
	t.Cleanup(func() {
		select {
		case <-h.hold:
		default:
			close(h.hold)
		}
		srv.Close()
	})
	h.url = "ws" + strings.TrimPrefix(srv.URL, "http")
	return h
}

func (h *pipeHub) next(ctx context.Context) *websocket.Conn {
	h.t.Helper()
	select {
	case conn := <-h.conns:
		return conn
	case <-ctx.Done():
		h.t.Fatal(ctx.Err())
		return nil
	}
}

func writeServer(ctx context.Context, conn *websocket.Conn, msg *collabv1.ServerMessage) error {
	b, err := proto.Marshal(msg)
	if err != nil {
		return err
	}
	return conn.Write(ctx, websocket.MessageBinary, b)
}

func readClient(ctx context.Context, conn *websocket.Conn) (*collabv1.ClientMessage, error) {
	typ, data, err := conn.Read(ctx)
	if err != nil {
		return nil, err
	}
	if typ != websocket.MessageBinary {
		return nil, errors.New("text frame")
	}
	msg := &collabv1.ClientMessage{}
	if err := proto.Unmarshal(data, msg); err != nil {
		return nil, err
	}
	return msg, nil
}

func readUntil(t *testing.T, ctx context.Context, conn *websocket.Conn, match func(*collabv1.ClientMessage) bool) *collabv1.ClientMessage {
	t.Helper()
	for {
		msg, err := readClient(ctx, conn)
		if err != nil {
			t.Fatal(err)
		}
		if match(msg) {
			return msg
		}
	}
}

func coversClock(entries []*crdtv1.VectorEntry, client, clock uint64) bool {
	for _, entry := range entries {
		if entry.GetClient() == client && entry.GetClock() >= clock {
			return true
		}
	}
	return false
}

func TestReportsOnResumeNotOnAck(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	h := startPipeHub(t)
	errCh := make(chan error, 1)
	var client *Client
	go func() {
		var err error
		client, err = DialWith(ctx, "doc", Options{URLs: []string{h.url}, Client: 1, ReportInterval: time.Hour})
		errCh <- err
	}()
	conn := h.next(ctx)
	if err := writeServer(ctx, conn, syncAt(0)); err != nil {
		t.Fatal(err)
	}
	if err := <-errCh; err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { client.Close() })

	if err := client.InsertAt(0, 'a'); err != nil {
		t.Fatal(err)
	}
	batch := readUntil(t, ctx, conn, func(m *collabv1.ClientMessage) bool { return m.GetOperationBatch() != nil })
	if err := writeServer(ctx, conn, &collabv1.ServerMessage{Payload: &collabv1.ServerMessage_OperationBroadcast{OperationBroadcast: &collabv1.OperationBroadcast{
		FirstSeq:   1,
		Operations: batch.GetOperationBatch().GetOperations(),
	}}}); err != nil {
		t.Fatal(err)
	}
	quiet, quietCancel := context.WithTimeout(ctx, 300*time.Millisecond)
	for {
		msg, err := readClient(quiet, conn)
		if err != nil {
			break
		}
		if msg.GetVectorReport() != nil {
			quietCancel()
			t.Fatal("an ack sent a VectorReport; reports go on resume and on the interval only")
		}
	}
	quietCancel()

	client.Disconnect()
	if err := client.InsertAt(1, 'b'); err != nil {
		t.Fatal(err)
	}
	reconn := make(chan error, 1)
	go func() { reconn <- client.Reconnect(ctx, h.url) }()
	conn = h.next(ctx)
	if err := writeServer(ctx, conn, &collabv1.ServerMessage{Payload: &collabv1.ServerMessage_MissedOps{MissedOps: &collabv1.MissedOps{
		FirstSeq: 2,
		Seq:      1,
	}}}); err != nil {
		t.Fatal(err)
	}
	if err := <-reconn; err != nil {
		t.Fatal(err)
	}

	var frames []*collabv1.ClientMessage
	deadline := time.Now().Add(2 * time.Second)
	for len(frames) < 2 && time.Now().Before(deadline) {
		readCtx, readCancel := context.WithTimeout(ctx, 200*time.Millisecond)
		msg, err := readClient(readCtx, conn)
		readCancel()
		if err != nil {
			continue
		}
		frames = append(frames, msg)
	}
	if len(frames) < 2 || frames[0].GetOperationBatch() == nil || frames[1].GetVectorReport() == nil {
		t.Fatalf("resume frames = %d", len(frames))
	}
	if !coversClock(frames[1].GetVectorReport().GetVector(), 1, 2) {
		t.Fatalf("resume report = %v", frames[1].GetVectorReport().GetVector())
	}
}

func TestReportsOnInterval(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	h := startPipeHub(t)
	errCh := make(chan error, 1)
	var client *Client
	go func() {
		var err error
		client, err = DialWith(ctx, "doc", Options{
			URLs:           []string{h.url},
			Client:         1,
			ReportInterval: 30 * time.Millisecond,
		})
		errCh <- err
	}()
	conn := h.next(ctx)
	if err := writeServer(ctx, conn, syncAt(0)); err != nil {
		t.Fatal(err)
	}
	if err := <-errCh; err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { client.Close() })
	report := readUntil(t, ctx, conn, func(m *collabv1.ClientMessage) bool { return m.GetVectorReport() != nil })
	if report.GetVectorReport() == nil {
		t.Fatal("interval produced no VectorReport")
	}
}
