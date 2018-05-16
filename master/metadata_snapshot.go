package master

import (
	"github.com/tecbot/gorocksdb"
	"io"
)

type MetadataSnapshot struct {
	fsm      *MetadataFsm
	applied  uint64
	snapshot *gorocksdb.Snapshot
	iterator *gorocksdb.Iterator
}

func (ms *MetadataSnapshot) ApplyIndex() uint64 {
	return ms.applied
}

func (ms *MetadataSnapshot) Close() {
	ms.fsm.store.ReleaseSnapshot(ms.snapshot)
}

func (ms *MetadataSnapshot) Next() (data []byte, err error) {

	if ms.iterator.Valid() {
		ms.iterator.Next()
		return data, nil
	}
	return nil, io.EOF
}
