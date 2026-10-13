package testdb

import (
	crdtv1 "github.com/rowjay007/collab-sphere/gen/crdt/v1"
	"github.com/rowjay007/collab-sphere/pkg/crdt"
)

func ProtoFromInsert(op crdt.Insert) *crdtv1.Operation {
	return crdt.OpToProto(op)
}
