package main

import (
	"testing"
	"time"

	"github.com/google/uuid"

	presencev1 "github.com/rowjay007/collab-sphere/gen/presence/v1"
	"github.com/rowjay007/collab-sphere/services/presence-service/internal/store"
	"github.com/rowjay007/collab-sphere/services/presence-service/internal/testredis"
)

func TestDrainSweepClearsBacklog(t *testing.T) {
	rdb := testredis.Client(t)
	st := store.New(rdb, 50*time.Millisecond)
	doc := uuid.NewString()
	changes := make([]*presencev1.Change, 0, 600)
	for client := uint64(1); client <= 600; client++ {
		changes = append(changes, &presencev1.Change{
			Kind: &presencev1.Change_Upsert{Upsert: &presencev1.State{
				DocumentId: doc,
				Client:     client,
				Session:    client,
			}},
		})
	}
	if _, _, _, err := st.Apply(t.Context(), changes); err != nil {
		t.Fatal(err)
	}
	time.Sleep(100 * time.Millisecond)
	if err := drainSweep(t.Context(), st, 256); err != nil {
		t.Fatal(err)
	}
	states, _, err := st.List(t.Context(), doc)
	if err != nil {
		t.Fatal(err)
	}
	if len(states) != 0 {
		t.Fatalf("%d states remain", len(states))
	}
}
