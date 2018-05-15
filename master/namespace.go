package master

import (
	"sync"
)

type NameSpace struct {
	Name              string
	volReplicaNum     uint8
	mpReplicaNum      uint8
	threshold         float32
	mpSize            uint64
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
	ns.mpSize = DefaultMetaPartitionMemSize
	if replicaNum%2 == 0 {
		ns.mpReplicaNum = replicaNum + 1
	} else {
		ns.mpReplicaNum = replicaNum
	}
	return
}

func (ns *NameSpace) AddMetaPartition(mp *MetaPartition) {
	exist := false
	for _, oldMp := range ns.MetaPartitions {
		if oldMp.Start == mp.Start && oldMp.End == mp.End {
			exist = true
		}
	}
	if !exist {
		ns.MetaPartitions[mp.PartitionID] = mp
	}
}

func (ns *NameSpace) GetMetaPartition(inode uint64) (mp *MetaPartition) {
	for _, mp = range ns.MetaPartitions {
		if mp.Start >= inode && mp.End < inode {
			return mp
		}
	}

	return nil
}

func (ns *NameSpace) getVolGroupByVolID(volID uint64) (vol *VolGroup, err error) {
	return ns.volGroups.getVol(volID)
}

func (ns *NameSpace) getMetaPartitionById(partitionID uint64) (mp *MetaPartition, err error) {
	mp, ok := ns.MetaPartitions[partitionID]
	if !ok {
		err = metaPartitionNotFound(partitionID)
	}
	return
}
