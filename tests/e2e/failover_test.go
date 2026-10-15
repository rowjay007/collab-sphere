package e2e

import (
	"context"
	"math/rand/v2"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/rowjay007/collab-sphere/pkg/collabclient"
	"github.com/rowjay007/collab-sphere/pkg/crdt"
	"github.com/rowjay007/collab-sphere/tests/internal/stack"
)

func TestHubCrashFailover(t *testing.T) {
	s := stack.New(t, stack.WithHubs(3))
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	docID := s.CreateDocument(ctx)

	clients := make([]*collabclient.Client, 12)
	for i := range clients {
		clients[i] = s.DialAuto(ctx, docID, crdt.ClientID(3001+i), i%s.Hubs())
	}

	stop := make(chan struct{})
	generated := make([][]crdt.ID, len(clients))
	var wg sync.WaitGroup
	for i, c := range clients {
		wg.Add(1)
		go func() {
			defer wg.Done()
			rng := rand.New(rand.NewPCG(uint64(i), 99))
			for {
				select {
				case <-stop:
					return
				case <-time.After(time.Duration(5+rng.IntN(20)) * time.Millisecond):
				}
				ids, err := tracked(c, typeOrDelete(rng))
				if err != nil {
					t.Error(err)
					return
				}
				generated[i] = append(generated[i], ids...)
			}
		}()
	}
	time.Sleep(time.Second)
	s.KillHub(1)
	time.Sleep(2 * time.Second)
	close(stop)
	wg.Wait()

	final := stack.Settle(t, ctx, s.Docs, docID, clients)
	all := slices.Concat(generated...)
	stack.AssertExactlyOnce(t, ctx, s.Docs, docID, all)

	var reconnected int
	for _, c := range clients {
		if c.LastJoin() == collabclient.Missed {
			reconnected++
		}
	}
	if reconnected == 0 {
		t.Fatal("no client resumed on another hub after hub 1 was killed")
	}
	t.Logf("%d ops from %d clients, %d resumed elsewhere, %d visible runes", len(all), len(clients), reconnected, len([]rune(final)))
}
