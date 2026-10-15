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

type TouchResult uint8

const (
	TouchMissing TouchResult = iota
	TouchApplied
	TouchSuperseded
)

type UpsertResult uint8

const (
	UpsertApplied UpsertResult = iota + 1
	UpsertSuperseded
)

func New(rdb *redis.Client, ttl time.Duration) *Store {
	return &Store{rdb: rdb, ttl: ttl}
}

func (s *Store) Join(ctx context.Context, st *presencev1.State) (UpsertResult, error) {
	raw, err := marshalState(st)
	if err != nil {
		return 0, err
	}
	result, err := joinUpsertScript.Run(ctx, s.rdb, []string{
		stateKey(st.GetDocumentId(), st.GetClient()),
		membersKey(st.GetDocumentId()),
		expiryKey,
		verKey(st.GetDocumentId()),
	}, s.upsertArgs(st, raw)...).Slice()
	if err != nil {
		return 0, err
	}
	return applyUpsertResult(st, result)
}

func (s *Store) Touch(ctx context.Context, t *presencev1.Touch) (TouchResult, error) {
	n, err := touchScript.Run(ctx, s.rdb, []string{
		stateKey(t.GetDocumentId(), t.GetClient()),
		expiryKey,
	}, uvarint(t.GetSession()), s.ttl.Milliseconds(), expiryMember(t.GetDocumentId(), t.GetClient()), s.pxMillis()).Int64()
	if err != nil {
		return TouchMissing, err
	}
	return TouchResult(n), nil
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

func (s *Store) Apply(ctx context.Context, changes []*presencev1.Change) (
	missing, superseded []*presencev1.Touch,
	supersededUpserts []*presencev1.State,
	err error,
) {
	type queued struct {
		change *presencev1.Change
		cmd    *redis.Cmd
	}

	rawStates := make(map[*presencev1.State][]byte)
	for _, ch := range changes {
		if upsert := ch.GetUpsert(); upsert != nil {
			raw, marshalErr := marshalState(upsert)
			if marshalErr != nil {
				return nil, nil, nil, marshalErr
			}
			rawStates[upsert] = raw
		}
	}

	pipe := s.rdb.Pipeline()
	queuedChanges := make([]queued, 0, len(changes))
	var queuedUpsert, queuedTouch, queuedLeave bool
	for _, ch := range changes {
		var cmd *redis.Cmd
		switch k := ch.GetKind().(type) {
		case *presencev1.Change_Upsert:
			keys := []string{
				stateKey(k.Upsert.GetDocumentId(), k.Upsert.GetClient()),
				membersKey(k.Upsert.GetDocumentId()),
				expiryKey,
				verKey(k.Upsert.GetDocumentId()),
			}
			if queuedUpsert {
				cmd = applyUpsertScript.EvalSha(ctx, pipe, keys, s.upsertArgs(k.Upsert, rawStates[k.Upsert])...)
			} else {
				cmd = applyUpsertScript.Eval(ctx, pipe, keys, s.upsertArgs(k.Upsert, rawStates[k.Upsert])...)
				queuedUpsert = true
			}
		case *presencev1.Change_Touch:
			keys := []string{
				stateKey(k.Touch.GetDocumentId(), k.Touch.GetClient()),
				expiryKey,
			}
			args := []any{uvarint(k.Touch.GetSession()), s.ttl.Milliseconds(),
				expiryMember(k.Touch.GetDocumentId(), k.Touch.GetClient()), s.pxMillis()}
			if queuedTouch {
				cmd = touchScript.EvalSha(ctx, pipe, keys, args...)
			} else {
				cmd = touchScript.Eval(ctx, pipe, keys, args...)
				queuedTouch = true
			}
		case *presencev1.Change_Leave:
			client := strconv.FormatUint(k.Leave.GetClient(), 10)
			keys := []string{
				stateKey(k.Leave.GetDocumentId(), k.Leave.GetClient()),
				membersKey(k.Leave.GetDocumentId()),
				expiryKey,
				verKey(k.Leave.GetDocumentId()),
			}
			args := []any{uvarint(k.Leave.GetSession()), client,
				expiryMember(k.Leave.GetDocumentId(), k.Leave.GetClient()),
				k.Leave.GetDocumentId(), uvarint(k.Leave.GetClient()),
				broadcast.Channel(k.Leave.GetDocumentId())}
			if queuedLeave {
				cmd = removeScript.EvalSha(ctx, pipe, keys, args...)
			} else {
				cmd = removeScript.Eval(ctx, pipe, keys, args...)
				queuedLeave = true
			}
		}
		queuedChanges = append(queuedChanges, queued{change: ch, cmd: cmd})
	}
	if _, err = pipe.Exec(ctx); err != nil {
		return nil, nil, nil, err
	}

	for _, queuedChange := range queuedChanges {
		switch k := queuedChange.change.GetKind().(type) {
		case *presencev1.Change_Upsert:
			result, sliceErr := queuedChange.cmd.Slice()
			if sliceErr != nil {
				return nil, nil, nil, sliceErr
			}
			upsertResult, resultErr := applyUpsertResult(k.Upsert, result)
			if resultErr != nil {
				return nil, nil, nil, resultErr
			}
			if upsertResult == UpsertSuperseded {
				supersededUpserts = append(supersededUpserts, k.Upsert)
			}
		case *presencev1.Change_Touch:
			n, intErr := queuedChange.cmd.Int64()
			if intErr != nil {
				return nil, nil, nil, intErr
			}
			switch TouchResult(n) {
			case TouchMissing:
				missing = append(missing, k.Touch)
			case TouchSuperseded:
				superseded = append(superseded, k.Touch)
			}
		}
	}
	return missing, superseded, supersededUpserts, nil
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

func (s *Store) upsertArgs(st *presencev1.State, raw []byte) []any {
	client := strconv.FormatUint(st.GetClient(), 10)
	// Presence epochs are connection counters, so Lua's exact integer range is ample.
	return []any{
		raw, s.ttl.Milliseconds(), client, expiryMember(st.GetDocumentId(), st.GetClient()),
		st.GetDocumentId(), uvarint(st.GetClient()), broadcast.Channel(st.GetDocumentId()),
		s.pxMillis(), uvarint(st.GetSession()), st.GetEpoch(),
	}
}

func applyUpsertResult(st *presencev1.State, raw []any) (UpsertResult, error) {
	if len(raw) != 2 {
		return 0, fmt.Errorf("want upsert result pair, got %d values", len(raw))
	}
	status, err := asUint64(raw[0])
	if err != nil {
		return 0, err
	}
	ver, err := asUint64(raw[1])
	if err != nil {
		return 0, err
	}
	result := UpsertResult(status)
	if result == UpsertApplied {
		st.Version = ver
	}
	return result, nil
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
