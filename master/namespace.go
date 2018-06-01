package master

import (
	"sync"
)

type NameSpace struct {
	Name              string
	volReplicaNum     uint8
	mpReplicaNum      uint8
	threshold         float32
	MetaPartitions    map[uint64]*MetaPartition
	metaPartitionLock sync.RWMutex
	volGroups         *VolGroupMap
	sync.Mutex
}

func NewNameSpace(name string, replicaNum uint8) (ns *NameSpace) {
	ns = &NameSpace{Name: name, MetaPartitions: make(map[uint64]*MetaPartition, 0)}
	ns.volGroups = NewVolMap()
	ns.volReplicaNum = replicaNum
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

func (ns *NameSpace) getVolGroupByVolID(volID uint64) (vol *VolGroup, err error) {
	return ns.volGroups.getVol(volID)
}

func (ns *NameSpace) getMetaPartition(partitionID uint64) (mp *MetaPartition, err error) {
	mp, ok := ns.MetaPartitions[partitionID]
	if !ok {
		err = metaPartitionNotFound(partitionID)
	}
	return
}

func (ns *NameSpace) getVolsView() (body []byte, err error) {
	return ns.volGroups.updateVolResponseCache(false, 0)
}
