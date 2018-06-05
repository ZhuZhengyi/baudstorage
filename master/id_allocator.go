package master

import (
	"fmt"
	"github.com/tiglabs/baudstorage/raftstore"
	"github.com/tiglabs/baudstorage/util/log"
	"strconv"
	"sync"
	"sync/atomic"
)

const (
	MaxDataPartitionIDKey = "max_dp_id"
	MaxMetaPartitionIDKey = "max_mp_id"
	MaxMetaNodeIDKey      = "max_metaNode_id"
)

type IDAllocator struct {
	dataPartitionID     uint64
	metaPartitionID     uint64
	metaNodeID          uint64
	dataPartitionIDLock sync.Mutex
	metaPartitionIDLock sync.Mutex
	metaNodeIDLock      sync.Mutex
	store               *raftstore.RocksDBStore
	partition           raftstore.Partition
}

func newIDAllocator(store *raftstore.RocksDBStore, partition raftstore.Partition) (alloc *IDAllocator) {
	alloc = new(IDAllocator)
	alloc.store = store
	alloc.partition = partition
	return
}

func (alloc *IDAllocator) restore() {
	alloc.restoreMaxDataPartitionID()
	alloc.restoreMaxMetaPartitionID()
	alloc.restoreMaxMetaNodeID()
}

func (alloc *IDAllocator) restoreMaxDataPartitionID() {
	value, err := alloc.store.Get(MaxDataPartitionIDKey)
	bytes := value.([]byte)
	if err != nil {
		panic(fmt.Sprintf("Failed to restore maxDataPartitionId,err:%v ", err.Error()))
	}

	if len(bytes) == 0 {
		alloc.dataPartitionID = 0
		return
	}
	maxDataPartitionId, err := strconv.ParseUint(string(bytes), 10, 64)
	if err != nil {
		panic(fmt.Sprintf("Failed to restore maxDataPartitionId,err:%v ", err.Error()))
	}
	alloc.dataPartitionID = maxDataPartitionId
}

func (alloc *IDAllocator) restoreMaxMetaPartitionID() {
	value, err := alloc.store.Get(MaxMetaPartitionIDKey)
	bytes := value.([]byte)
	if err != nil {
		panic(fmt.Sprintf("Failed to restore maxPartitionID,err:%v ", err.Error()))
	}

	if len(bytes) == 0 {
		alloc.metaPartitionID = 0
		return
	}
	maxPartitionID, err := strconv.ParseUint(string(bytes), 10, 64)
	if err != nil {
		panic(fmt.Sprintf("Failed to restore maxPartitionID,err:%v ", err.Error()))
	}
	alloc.metaPartitionID = maxPartitionID

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

func (alloc *IDAllocator) allocateDataPartitionID() (ID uint64, err error) {
	alloc.dataPartitionIDLock.Lock()
	defer alloc.dataPartitionIDLock.Unlock()
	var cmd []byte
	metadata := new(Metadata)
	ID = atomic.AddUint64(&alloc.dataPartitionID, 1)
	metadata.K = MaxDataPartitionIDKey
	value := strconv.FormatUint(uint64(ID), 10)
	metadata.V = []byte(value)
	cmd, err = metadata.Marshal()
	if err != nil {
		goto errDeal
	}
	if _, err = alloc.partition.Submit(cmd); err != nil {
		goto errDeal
	}
	return
errDeal:
	log.LogError("action[allocateDataPartitionID] err:%v", err.Error())
	return
}

func (alloc *IDAllocator) allocateMetaPartitionID() (partitionID uint64, err error) {
	var cmd []byte
	alloc.metaPartitionIDLock.Lock()
	defer alloc.metaPartitionIDLock.Unlock()
	metadata := new(Metadata)
	metadata.K = MaxMetaPartitionIDKey
	partitionID = atomic.AddUint64(&alloc.metaPartitionID, 1)
	value := strconv.FormatUint(uint64(partitionID), 10)
	metadata.V = []byte(value)
	cmd, err = metadata.Marshal()
	if err != nil {
		goto errDeal
	}
	if _, err = alloc.partition.Submit(cmd); err != nil {
		goto errDeal
	}
	return
errDeal:
	log.LogError("action[allocateMetaPartitionID] err:%v", err.Error())
	return
}

func (alloc *IDAllocator) allocateMetaNodeID() (metaNodeID uint64, err error) {
	var cmd []byte
	alloc.metaNodeIDLock.Lock()
	defer alloc.metaNodeIDLock.Unlock()
	metadata := new(Metadata)
	metadata.K = MaxMetaNodeIDKey
	metaNodeID = atomic.AddUint64(&alloc.metaNodeID, 1)
	value := strconv.FormatUint(uint64(metaNodeID), 10)
	metadata.V = []byte(value)
	cmd, err = metadata.Marshal()
	if err != nil {
		goto errDeal
	}
	if _, err = alloc.partition.Submit(cmd); err != nil {
		goto errDeal
	}
	return
errDeal:
	log.LogError("action[allocateMetaNodeID] err:%v", err.Error())
	return
}
