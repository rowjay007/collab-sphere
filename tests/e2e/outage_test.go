package e2e

import (
	"context"
	"fmt"
	"math/rand/v2"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/rowjay007/collab-sphere/pkg/collabclient"
	"github.com/rowjay007/collab-sphere/pkg/crdt"
	"github.com/rowjay007/collab-sphere/tests/internal/stack"
)

func TestRedisOutageHeals(t *testing.T) {
	s := stack.New(t, stack.WithHubs(3), stack.WithRedisProxy())
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	docID := s.CreateDocument(ctx)

	clients := make([]*collabclient.Client, 30)
	for i := range clients {
		clients[i] = s.DialAuto(ctx, docID, editor(i), i%s.Hubs())
		clients[i].SetName(fmt.Sprintf("editor-%d", i))
	}

	stop := make(chan struct{})
	generated := make([][]crdt.ID, len(clients))
	var wg sync.WaitGroup
	for i, c := range clients {
		wg.Go(func() {
			rng := rand.New(rand.NewPCG(uint64(i), 5))
			edit := typeOrDelete(rng)
			for {
				select {
				case <-stop:
					return
				case <-time.After(time.Duration(10+rng.IntN(30)) * time.Millisecond):
				}
				var inserted crdt.ID
				ids, err := tracked(c, func(r *crdt.Replica) (crdt.Op, error) {
					op, err := edit(r)
					if ins, ok := op.(crdt.Insert); ok && err == nil {
						inserted = ins.ID
					}
					return op, err
				})
				if err != nil {
					t.Error(err)
					return
				}
				generated[i] = append(generated[i], ids...)
				if inserted != (crdt.ID{}) {
					_ = c.MoveCursor(inserted, inserted)
				}
			}
		})
	}
	time.Sleep(time.Second)
	s.CutRedis()
	time.Sleep(2 * time.Second)
	s.RestoreRedis()
	time.Sleep(2 * time.Second)
	close(stop)
	wg.Wait()

	stack.Settle(t, ctx, s.Docs, docID, clients)
	all := slices.Concat(generated...)
	stack.AssertExactlyOnce(t, ctx, s.Docs, docID, all)

	deadline := time.Now().Add(10 * time.Second)
	for diff := presenceDisagreement(clients); diff != ""; diff = presenceDisagreement(clients) {
		if time.Now().After(deadline) {
			t.Fatalf("presence views never agreed: %s", diff)
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Logf("%d ops from %d editors across a 2s Redis cut", len(all), len(clients))
}

func editor(i int) crdt.ClientID { return crdt.ClientID(5001 + i) }

// presenceDisagreement describes the first way the editors' presence views
// differ: each must see every other editor, by name, exactly as the others
// see it.
func presenceDisagreement(clients []*collabclient.Client) string {
	views := make([]map[crdt.ClientID]collabclient.PresenceView, len(clients))
	for i, c := range clients {
		views[i] = c.Presence()
	}
	for j := range clients {
		want, ok := views[(j+1)%len(clients)][editor(j)]
		if !ok || want.Name != fmt.Sprintf("editor-%d", j) {
			return fmt.Sprintf("editor %d sees editor %d as %+v (present %v)", (j+1)%len(clients), j, want, ok)
		}
		for i, view := range views {
			if i == j {
				continue
			}
			if got := view[editor(j)]; got != want {
				return fmt.Sprintf("editor %d sees editor %d as %+v, editor %d as %+v", i, j, got, (j+1)%len(clients), want)
			}
		}
	}
	for i, view := range views {
		if len(view) != len(clients)-1 {
			return fmt.Sprintf("editor %d sees %d others", i, len(view))
		}
	}
	return ""
}
