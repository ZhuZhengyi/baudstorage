package master

import (
	"sync"
)

type NameSpace struct {
	Name          string
	volReplicaNum uint8
	mrReplicaNum  uint8
	threshold     float32
	mrSize        uint64
	MetaGroups    map[uint64]*MetaGroup
	metaGroupLock sync.RWMutex
	volGroups     *VolGroupMap
	sync.Mutex
}

func NewNameSpace(name string, replicaNum uint8) (ns *NameSpace) {
	ns = &NameSpace{Name: name, MetaGroups: make(map[uint64]*MetaGroup, 0)}
	ns.volGroups = NewVolMap()
	ns.volReplicaNum = replicaNum
	ns.threshold = DefaultMetaRangeThreshold
	ns.mrSize = DefaultMetaRangeMemSize
	if replicaNum%2 == 0 {
		ns.mrReplicaNum = replicaNum + 1
	} else {
		ns.mrReplicaNum = replicaNum
	}
	return
}

func (ns *NameSpace) AddMetaGroup(mg *MetaGroup) {
	exist := false
	for _, omg := range ns.MetaGroups {
		if omg.Start == mg.Start && omg.End == mg.End {
			exist = true
		}
	}
	if !exist {
		ns.MetaGroups[mg.GroupID] = mg
	}
}

func (ns *NameSpace) GetMetaGroup(inode uint64) (mg *MetaGroup) {
	for _, mg = range ns.MetaGroups {
		if mg.Start >= inode && mg.End < inode {
			return mg
		}
	}

	return nil
}

func (ns *NameSpace) getVolGroupByVolID(volID uint64) (vol *VolGroup, err error) {
	return ns.volGroups.getVol(volID)
}

func (ns *NameSpace) getMetaGroupById(groupId uint64) (mg *MetaGroup, err error) {
	mg, ok := ns.MetaGroups[groupId]
	if !ok {
		err = metaGroupNotFound(groupId)
	}
	return
}
