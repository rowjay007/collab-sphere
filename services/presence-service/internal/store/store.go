package store

import (
	"context"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
	"google.golang.org/protobuf/proto"

	presencev1 "github.com/rowjay007/collab-sphere/gen/presence/v1"
	"github.com/rowjay007/collab-sphere/services/presence-service/internal/broadcast"
)

type Store struct {
	rdb *redis.Client
	ttl time.Duration
}

func New(rdb *redis.Client, ttl time.Duration) *Store {
	return &Store{rdb: rdb, ttl: ttl}
}

func (s *Store) Upsert(ctx context.Context, st *presencev1.State) error {
	raw, err := marshalState(st)
	if err != nil {
		return err
	}
	client := strconv.FormatUint(st.GetClient(), 10)
	// PX outlives the zset deadline so sweep can still read session after expiry.
	ver, err := upsertScript.Run(ctx, s.rdb, []string{
		stateKey(st.GetDocumentId(), st.GetClient()),
		membersKey(st.GetDocumentId()),
		expiryKey,
		verKey(st.GetDocumentId()),
	}, raw, s.ttl.Milliseconds(), client, expiryMember(st.GetDocumentId(), st.GetClient()),
		st.GetDocumentId(), uvarint(st.GetClient()), broadcast.Channel(st.GetDocumentId()), s.pxMillis()).Int64()
	if err != nil {
		return err
	}
	st.Version = uint64(ver)
	return nil
}

func (s *Store) Touch(ctx context.Context, t *presencev1.Touch) (bool, error) {
	n, err := touchScript.Run(ctx, s.rdb, []string{
		stateKey(t.GetDocumentId(), t.GetClient()),
		expiryKey,
	}, uvarint(t.GetSession()), s.ttl.Milliseconds(), expiryMember(t.GetDocumentId(), t.GetClient()), s.pxMillis()).Int64()
	if err != nil {
		return false, err
	}
	return n == 1, nil
}

func (s *Store) Remove(ctx context.Context, l *presencev1.Leave) error {
	client := strconv.FormatUint(l.GetClient(), 10)
	return removeScript.Run(ctx, s.rdb, []string{
		stateKey(l.GetDocumentId(), l.GetClient()),
		membersKey(l.GetDocumentId()),
		expiryKey,
		verKey(l.GetDocumentId()),
	}, uvarint(l.GetSession()), client, expiryMember(l.GetDocumentId(), l.GetClient()),
		l.GetDocumentId(), uvarint(l.GetClient()), broadcast.Channel(l.GetDocumentId())).Err()
}

func (s *Store) List(ctx context.Context, docID string) ([]*presencev1.State, uint64, error) {
	raw, err := listScript.Run(ctx, s.rdb, []string{
		membersKey(docID),
		expiryKey,
		verKey(docID),
	}, docID).Slice()
	if err != nil {
		return nil, 0, err
	}
	if len(raw) == 0 {
		return nil, 0, nil
	}
	ver, err := asUint64(raw[0])
	if err != nil {
		return nil, 0, err
	}
	states := make([]*presencev1.State, 0, len(raw)-1)
	for _, item := range raw[1:] {
		b, err := asBytes(item)
		if err != nil {
			return nil, 0, err
		}
		st := &presencev1.State{}
		if err := proto.Unmarshal(b, st); err != nil {
			return nil, 0, err
		}
		states = append(states, st)
	}
	return states, ver, nil
}

func (s *Store) pxMillis() int64 {
	return s.ttl.Milliseconds() + 2000
}

func (s *Store) Sweep(ctx context.Context, limit int) (int, error) {
	n, err := sweepScript.Run(ctx, s.rdb, []string{expiryKey}, limit).Int()
	return n, err
}

func marshalState(st *presencev1.State) ([]byte, error) {
	cp := proto.Clone(st).(*presencev1.State)
	cp.Version = 0
	return proto.Marshal(cp)
}

func uvarint(n uint64) []byte {
	var buf [binary.MaxVarintLen64]byte
	return append([]byte(nil), buf[:binary.PutUvarint(buf[:], n)]...)
}

func stateKey(docID string, client uint64) string {
	return "presence:" + docID + ":" + strconv.FormatUint(client, 10)
}

func membersKey(docID string) string {
	return "presence-members:" + docID
}

func verKey(docID string) string {
	return "presence-ver:" + docID
}

func expiryMember(docID string, client uint64) string {
	return docID + "|" + strconv.FormatUint(client, 10) + "|" + hex.EncodeToString(uvarint(client))
}

const expiryKey = "presence-expiry"

func asBytes(v any) ([]byte, error) {
	switch x := v.(type) {
	case []byte:
		return x, nil
	case string:
		return []byte(x), nil
	default:
		return nil, fmt.Errorf("want bytes, got %T", v)
	}
}

func asUint64(v any) (uint64, error) {
	switch x := v.(type) {
	case int64:
		return uint64(x), nil
	case uint64:
		return x, nil
	case string:
		return strconv.ParseUint(x, 10, 64)
	case []byte:
		return strconv.ParseUint(string(x), 10, 64)
	default:
		return 0, fmt.Errorf("want uint64, got %T", v)
	}
}
