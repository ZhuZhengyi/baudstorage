package master

import (
	"fmt"
	"github.com/tiglabs/baudstorage/raftstore"
	"strconv"
	"sync"
	"sync/atomic"
)

const (
	MaxVolIDKey       = "max_vol_id"
	MaxPartitionIDKey = "max_partition_id"
	MaxMetaNodeIDKey  = "max_metaNode_id"
)

type IDAllocator struct {
	volID           uint64
	partitionID     uint64
	metaNodeID      uint64
	volIDLock       sync.Mutex
	partitionIDLock sync.Mutex
	metaNodeIDLock  sync.Mutex
	store           *raftstore.RocksDBStore
	partition       raftstore.Partition
}

func newIDAllocator(store *raftstore.RocksDBStore, partition raftstore.Partition) (alloc *IDAllocator) {
	alloc = new(IDAllocator)
	alloc.store = store
	alloc.partition = partition
	return
}

func (alloc *IDAllocator) restore() {
	alloc.restoreMaxMetaNodeID()
	alloc.restoreMaxPartitionID()
	alloc.restoreMaxMetaNodeID()
}

func (alloc *IDAllocator) restoreMaxVolID() {
	value, err := alloc.store.Get(MaxVolIDKey)
	bytes := value.([]byte)
	if err != nil {
		panic(fmt.Sprintf("Failed to restore maxVolId,err:%v ", err.Error()))
	}

	if len(bytes) == 0 {
		alloc.volID = 0
		return
	}
	maxVolId, err := strconv.ParseUint(string(bytes), 10, 64)
	if err != nil {
		panic(fmt.Sprintf("Failed to restore maxVolId,err:%v ", err.Error()))
	}
	alloc.volID = maxVolId
}

func (alloc *IDAllocator) restoreMaxPartitionID() {
	value, err := alloc.store.Get(MaxPartitionIDKey)
	bytes := value.([]byte)
	if err != nil {
		panic(fmt.Sprintf("Failed to restore maxPartitionID,err:%v ", err.Error()))
	}

	if len(bytes) == 0 {
		alloc.partitionID = 0
		return
	}
	maxPartitionID, err := strconv.ParseUint(string(bytes), 10, 64)
	if err != nil {
		panic(fmt.Sprintf("Failed to restore maxPartitionID,err:%v ", err.Error()))
	}
	alloc.partitionID = maxPartitionID

}

func (alloc *IDAllocator) restoreMaxMetaNodeID() {
	value, err := alloc.store.Get(MaxMetaNodeIDKey)
	bytes := value.([]byte)
	if err != nil {
		panic(fmt.Sprintf("Failed to restore maxMetaNodeID,err:%v ", err.Error()))
	}

	if len(bytes) == 0 {
		alloc.metaNodeID = 0
		return
	}
	maxMetaNodeID, err := strconv.ParseUint(string(bytes), 10, 64)
	if err != nil {
		panic(fmt.Sprintf("Failed to restore maxMetaNodeID,err:%v ", err.Error()))
	}
	alloc.metaNodeID = maxMetaNodeID

}

func (alloc *IDAllocator) allocatorVolID() (volID uint64, err error) {
	alloc.volIDLock.Lock()
	defer alloc.volIDLock.Unlock()
	metadata := new(Metadata)
	volID = atomic.AddUint64(&alloc.volID, 1)
	cmd, err := metadata.Marshal()
	if err != nil {
		goto errDeal
	}
	if _, err := alloc.partition.Submit(cmd); err != nil {
		goto errDeal
	}
	return
errDeal:
	atomic.AddUint64(&alloc.volID, -1)
	return
}

func (alloc *IDAllocator) allocatorPartitionID() (partitionID uint64, err error) {
	alloc.partitionIDLock.Lock()
	defer alloc.partitionIDLock.Unlock()
	metadata := new(Metadata)
	partitionID = atomic.AddUint64(&alloc.partitionID, 1)
	cmd, err := metadata.Marshal()
	if err != nil {
		goto errDeal
	}
	if _, err := alloc.partition.Submit(cmd); err != nil {
		goto errDeal
	}
	return
errDeal:
	atomic.AddUint64(&alloc.partitionID, -1)
	return
}

func (alloc *IDAllocator) allocatorMetaNodeID() (metaNodeID uint64, err error) {
	alloc.metaNodeIDLock.Lock()
	defer alloc.metaNodeIDLock.Unlock()
	metadata := new(Metadata)
	metaNodeID = atomic.AddUint64(&alloc.metaNodeID, 1)
	cmd, err := metadata.Marshal()
	if err != nil {
		goto errDeal
	}
	if _, err := alloc.partition.Submit(cmd); err != nil {
		goto errDeal
	}
	return
errDeal:
	atomic.AddUint64(&alloc.metaNodeID, -1)
	return
}
