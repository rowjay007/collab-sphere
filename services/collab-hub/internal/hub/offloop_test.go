package hub

import (
	"bytes"
	"context"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"

	collabv1 "github.com/rowjay007/collab-sphere/gen/collab/v1"
	permissionv1 "github.com/rowjay007/collab-sphere/gen/permission/v1"
	"github.com/rowjay007/collab-sphere/pkg/crdt"
)

func TestReadDoesNotBlockApply(t *testing.T) {
	base := NewMemoryStore()
	store := &blockingReadStore{
		Store:   base,
		started: make(chan struct{}),
		release: make(chan struct{}),
	}
	reg := localRegistry(store)
	reg.idleSync = 15 * time.Millisecond
	t.Cleanup(reg.Close)
	t.Cleanup(func() {
		select {
		case <-store.release:
		default:
			close(store.release)
		}
	})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	session, _, stream := join(t, ctx, reg, 1)
	select {
	case <-store.started:
	case <-ctx.Done():
		t.Fatal("ReadOperations did not start")
	}
	start := time.Now()
	session.SubmitOps(ctx, []crdt.Op{insert(1, 1)})
	echoCtx, echoCancel := context.WithTimeout(ctx, 100*time.Millisecond)
	defer echoCancel()
	if err := stream.waitSeq(echoCtx, 1); err != nil {
		t.Fatalf("echo during blocked ReadOperations: %v", err)
	}
	if elapsed := time.Since(start); elapsed >= 100*time.Millisecond {
		t.Fatalf("echo took %s, want under 100ms", elapsed)
	}
	select {
	case <-store.release:
		t.Fatal("read was released before the echo")
	default:
	}
}

func TestJoinDuringBlockedFillDeliversInOrder(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	base := NewMemoryStore()
	store := &blockingReadStore{
		Store:   base,
		started: make(chan struct{}),
		release: make(chan struct{}),
	}
	reg := localRegistry(store)
	reg.idleSync = time.Hour
	reg.gapGrace = time.Hour
	t.Cleanup(reg.Close)
	t.Cleanup(func() {
		select {
		case <-store.release:
		default:
			close(store.release)
		}
	})

	alice, _, aliceStream := join(t, ctx, reg, 1)
	alice.SubmitOps(ctx, []crdt.Op{insert(1, 1)})
	if err := aliceStream.waitSeq(ctx, 1); err != nil {
		t.Fatal(err)
	}
	if _, err := base.AppendFromOtherWriter("doc", insert(50, 1)); err != nil {
		t.Fatal(err)
	}
	alice.SubmitOps(ctx, []crdt.Op{insert(1, 2)})
	for {
		if _, meta, _ := base.ReadOperations(ctx, "doc", 0, 10); meta.OpSeq == 3 {
			break
		}
		select {
		case <-ctx.Done():
			t.Fatal("third op was never appended")
		case <-time.After(time.Millisecond):
		}
	}
	if seq, _, _, _ := aliceStream.snapshot(); seq != 1 {
		t.Fatalf("gap closed before the fill: alice at seq %d", seq)
	}

	bob := NewPeer(2, 0)
	bobJoined := make(chan error, 1)
	go func() {
		_, err := reg.Join(ctx, "doc", bob, false, 0)
		bobJoined <- err
	}()
	select {
	case <-store.started:
	case <-ctx.Done():
		t.Fatal("fill did not start")
	}
	select {
	case err := <-bobJoined:
		t.Fatalf("fresh join finished before the blocked read returned: %v", err)
	default:
	}

	carol := NewPeer(3, 0)
	carolJoined := make(chan error, 1)
	go func() {
		_, err := reg.Join(ctx, "doc", carol, true, 0)
		carolJoined <- err
	}()
	time.Sleep(20 * time.Millisecond)
	select {
	case err := <-carolJoined:
		t.Fatalf("resume finished before the blocked read returned: %v", err)
	default:
	}
	close(store.release)

	if err := <-bobJoined; err != nil {
		t.Fatal(err)
	}
	if err := <-carolJoined; err != nil {
		t.Fatal(err)
	}
	if err := aliceStream.waitSeq(ctx, 3); err != nil {
		t.Fatal(err)
	}

	log, _, err := base.ReadOperations(ctx, "doc", 0, 10)
	if err != nil {
		t.Fatal(err)
	}
	syncMsg := readServerMessage(t, ctx, bob).GetDocumentSync()
	if syncMsg.GetSeq() != 3 {
		t.Fatalf("DocumentSync.seq = %d, want 3", syncMsg.GetSeq())
	}
	if want := logPrefix(t, base, 3).Snapshot(); !proto.Equal(syncMsg.GetSnapshot(), want) {
		t.Fatal("snapshot differs from log[1..3]")
	}
	frames, ids := readMissed(t, ctx, carol, 0)
	if got, want := missedBytes(t, frames), opBytes(t, log.Ops); !bytesEqual(got, want) {
		t.Fatalf("MissedOps bytes differ from the log")
	}
	if len(ids) != 3 {
		t.Fatalf("MissedOps holds %d ops, want 3", len(ids))
	}

	alice.SubmitOps(ctx, []crdt.Op{insert(1, 3)})
	for _, peer := range []*Peer{bob, carol} {
		msg := readServerMessage(t, ctx, peer)
		b := msg.GetOperationBroadcast()
		if b.GetFirstSeq() != 4 || len(b.GetOperations()) != 1 {
			t.Fatalf("next frame %+v, want the new op at seq 4", b)
		}
	}
}

func TestMissedOpsPrecedeLiveOps(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	base := NewMemoryStore()
	prior := chain(50, 2)
	if _, err := base.AppendFromOtherWriter("doc", prior...); err != nil {
		t.Fatal(err)
	}
	store := &blockingReadStore{
		Store:   base,
		started: make(chan struct{}),
		release: make(chan struct{}),
	}
	reg := localRegistry(store)
	reg.idleSync = time.Hour
	t.Cleanup(reg.Close)
	t.Cleanup(func() {
		select {
		case <-store.release:
		default:
			close(store.release)
		}
	})

	alice, _, aliceStream := join(t, ctx, reg, 1)
	bob := NewPeer(2, 0)
	joined := make(chan error, 1)
	go func() {
		_, err := reg.Join(ctx, "doc", bob, true, 0)
		joined <- err
	}()
	select {
	case <-store.started:
	case <-ctx.Done():
		t.Fatal("MissedOps read did not start")
	}
	live := insert(1, 1)
	alice.SubmitOps(ctx, []crdt.Op{live})
	if err := aliceStream.waitSeq(ctx, 3); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-joined:
		t.Fatalf("resume finished before its MissedOps read returned: %v", err)
	default:
	}
	close(store.release)
	if err := <-joined; err != nil {
		t.Fatal(err)
	}

	frames, ids := readMissed(t, ctx, bob, 0)
	if frames[0].GetSeq() != 2 {
		t.Fatalf("MissedOps seq = %d, want 2", frames[0].GetSeq())
	}
	if got, want := missedBytes(t, frames), opBytes(t, prior); !bytesEqual(got, want) {
		t.Fatal("MissedOps bytes differ from the log prefix")
	}
	if len(ids) != len(prior) {
		t.Fatalf("MissedOps holds %d ops, want %d", len(ids), len(prior))
	}
	msg := readServerMessage(t, ctx, bob)
	b := msg.GetOperationBroadcast()
	if b.GetFirstSeq() != 3 || len(b.GetOperations()) != 1 {
		t.Fatalf("frame after MissedOps is %+v, want live op at seq 3", b)
	}
	got, err := proto.Marshal(b.GetOperations()[0])
	if err != nil {
		t.Fatal(err)
	}
	if want := opBytes(t, []crdt.Op{live}); !bytes.Equal(got, want[0]) {
		t.Fatal("live op bytes differ from the submitted op")
	}
}

func TestIdleFillDeliversLogBytes(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	store := NewMemoryStore()
	reg := localRegistry(store)
	reg.idleSync = 20 * time.Millisecond
	t.Cleanup(reg.Close)

	_, _, stream := join(t, ctx, reg, 1)
	op := insert(50, 1)
	if _, err := store.AppendFromOtherWriter("doc", op); err != nil {
		t.Fatal(err)
	}
	if err := stream.waitSeq(ctx, 1); err != nil {
		t.Fatal(err)
	}
	_, _, _, frames := stream.snapshot()
	var got []byte
	for _, f := range frames {
		b := f.GetOperationBroadcast()
		if b.GetFirstSeq() != 1 || len(b.GetOperations()) != 1 {
			continue
		}
		var err error
		got, err = proto.Marshal(b.GetOperations()[0])
		if err != nil {
			t.Fatal(err)
		}
	}
	if want := opBytes(t, []crdt.Op{op}); len(got) == 0 || !bytes.Equal(got, want[0]) {
		t.Fatal("idle fill broadcast bytes differ from the log")
	}
}

func TestPolicyWaitDoesNotBlockEcho(t *testing.T) {
	reg := localRegistry(NewMemoryStore())
	reg.idleSync = time.Hour
	t.Cleanup(reg.Close)
	policy := newMemberPolicy("ada")
	reg.SetSecurity(nil, policy)
	t.Cleanup(func() {
		select {
		case <-policy.release:
		default:
			close(policy.release)
		}
	})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	alice := NewPeer(1, 0)
	alice.SetUser("ada")
	session, err := reg.Join(ctx, "doc", alice, false, 0)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { session.Leave(context.Background()) })
	if readServerMessage(t, ctx, alice).GetDocumentSync() == nil {
		t.Fatal("join did not send a document sync")
	}
	policy.armNextBlock()
	select {
	case <-policy.started:
	case <-time.After(2 * time.Second):
		t.Fatal("policy dump did not block")
	}
	policy.setMembers(map[string]permissionv1.Role{
		"ada": permissionv1.Role_ROLE_EDITOR,
		"bob": permissionv1.Role_ROLE_EDITOR,
	})
	bob := NewPeer(2, 0)
	bob.SetUser("bob")
	joined := make(chan error, 1)
	go func() {
		_, err := reg.Join(ctx, "doc", bob, false, 0)
		joined <- err
	}()
	time.Sleep(50 * time.Millisecond)

	start := time.Now()
	session.SubmitOps(ctx, []crdt.Op{insert(1, 1)})
	echoCtx, echoCancel := context.WithTimeout(ctx, 100*time.Millisecond)
	defer echoCancel()
	msg := readServerMessage(t, echoCtx, alice)
	if msg.GetOperationBroadcast().GetFirstSeq() != 1 {
		t.Fatalf("echo frame %+v", msg.GetOperationBroadcast())
	}
	if elapsed := time.Since(start); elapsed >= 100*time.Millisecond {
		t.Fatalf("echo during policy wait took %s", elapsed)
	}
	select {
	case err := <-joined:
		t.Fatalf("join finished while the fresh policy was blocked: %v", err)
	default:
	}
	close(policy.release)
	select {
	case err := <-joined:
		if err != nil {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal("join did not finish after the policy dump")
	}
}

func opBytes(t *testing.T, ops []crdt.Op) [][]byte {
	t.Helper()
	out := make([][]byte, len(ops))
	for i, op := range ops {
		b, err := proto.Marshal(crdt.OpToProto(op))
		if err != nil {
			t.Fatal(err)
		}
		out[i] = b
	}
	return out
}

func missedBytes(t *testing.T, frames []*collabv1.MissedOps) [][]byte {
	t.Helper()
	var out [][]byte
	for _, f := range frames {
		for _, op := range f.GetOperations() {
			b, err := proto.Marshal(op)
			if err != nil {
				t.Fatal(err)
			}
			out = append(out, b)
		}
	}
	return out
}

func bytesEqual(a, b [][]byte) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if !bytes.Equal(a[i], b[i]) {
			return false
		}
	}
	return true
}

func TestNotePolicyWakesEveryWaiter(t *testing.T) {
	h := &Hub{policyNote: make(chan struct{})}
	woke := make(chan struct{}, 3)
	ready := make(chan struct{}, 3)
	for range 3 {
		go func() {
			changed := h.policyChanged()
			ready <- struct{}{}
			<-changed
			woke <- struct{}{}
		}()
	}
	for range 3 {
		<-ready
	}
	h.notePolicy()
	for i := range 3 {
		select {
		case <-woke:
		case <-time.After(time.Second):
			t.Fatalf("waiter %d not woken by one note", i)
		}
	}
}
