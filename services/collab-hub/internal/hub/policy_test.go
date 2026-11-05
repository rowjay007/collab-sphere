package hub

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	collabv1 "github.com/rowjay007/collab-sphere/gen/collab/v1"
	permissionv1 "github.com/rowjay007/collab-sphere/gen/permission/v1"
)

type flakyPolicy struct {
	mu  sync.Mutex
	err error
}

func (p *flakyPolicy) Dump(context.Context, string) (map[string]permissionv1.Role, uint32, string, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.err != nil {
		return nil, 0, "", p.err
	}
	return map[string]permissionv1.Role{"ada": permissionv1.Role_ROLE_EDITOR}, 1, "live", nil
}

func (p *flakyPolicy) fail(err error) {
	p.mu.Lock()
	p.err = err
	p.mu.Unlock()
}

func TestPolicyDumpStallStopsTheHub(t *testing.T) {
	reg := localRegistry(NewMemoryStore())
	t.Cleanup(reg.Close)
	policy := &flakyPolicy{}
	reg.SetSecurity(nil, policy)

	peer := NewPeer(1, 0)
	peer.SetUser("ada")
	if _, err := reg.Join(t.Context(), "doc", peer, false, 0); err != nil {
		t.Fatal(err)
	}
	if msg := readServerMessage(t, t.Context(), peer); msg.GetDocumentSync() == nil {
		t.Fatal("join did not send a document sync")
	}
	policy.fail(errors.New("permission service down"))

	ctx, cancel := context.WithTimeout(t.Context(), 4*time.Second)
	defer cancel()
	msg := readServerMessage(t, ctx, peer)
	if msg.GetError().GetCode() != collabv1.ErrorMessage_CODE_UNAVAILABLE {
		t.Fatalf("code = %s, want CODE_UNAVAILABLE", msg.GetError().GetCode())
	}
	select {
	case <-peer.Done():
	case <-ctx.Done():
		t.Fatal("peer stayed open after the permission dump went stale")
	}
}
