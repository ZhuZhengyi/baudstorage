package master

import (
	"sync"
)

type NameSpace struct {
	Name              string
	dpReplicaNum      uint8
	mpReplicaNum      uint8
	threshold         float32
	MetaPartitions    map[uint64]*MetaPartition
	metaPartitionLock sync.RWMutex
	dataPartitions    *DataPartitionMap
	sync.Mutex
}

func NewNameSpace(name string, replicaNum uint8) (ns *NameSpace) {
	ns = &NameSpace{Name: name, MetaPartitions: make(map[uint64]*MetaPartition, 0)}
	ns.dataPartitions = NewDataPartitionMap()
	ns.dpReplicaNum = replicaNum
	ns.threshold = DefaultMetaPartitionThreshold
	if replicaNum%2 == 0 {
		ns.mpReplicaNum = replicaNum + 1
	} else {
		ns.mpReplicaNum = replicaNum
	}
	return
}

func (ns *NameSpace) AddMetaPartition(mp *MetaPartition) {
	ns.metaPartitionLock.Lock()
	defer ns.metaPartitionLock.Unlock()
	if _, ok := ns.MetaPartitions[mp.PartitionID]; !ok {
		ns.MetaPartitions[mp.PartitionID] = mp
	}
}

func (ns *NameSpace) getDataPartitionByID(partitionID uint64) (dp *DataPartition, err error) {
	return ns.dataPartitions.getDataPartition(partitionID)
}

func (ns *NameSpace) getMetaPartition(partitionID uint64) (mp *MetaPartition, err error) {
	mp, ok := ns.MetaPartitions[partitionID]
	if !ok {
		err = metaPartitionNotFound(partitionID)
	}
	return
}

func (ns *NameSpace) getDataPartitionsView() (body []byte, err error) {
	return ns.dataPartitions.updateDataPartitionResponseCache(false, 0)
}
