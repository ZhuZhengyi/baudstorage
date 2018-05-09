package master

import "sync"

type NameSpace struct {
	Name       string
	replicaNum uint8
	MetaGroups map[string]*MetaGroup
	volGroups  *VolGroupMap
	sync.Mutex
}

func NewNameSpace(name string, replicaNum uint8) (ns *NameSpace) {
	ns = &NameSpace{Name: name, MetaGroups: make(map[string]*MetaGroup, 0)}
	ns.volGroups = NewVolMap()
	ns.replicaNum = replicaNum
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
