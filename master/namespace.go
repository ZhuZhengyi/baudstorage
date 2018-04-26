package master

import "sync"

type NameSpace struct {
	Name       string
	MetaGroups map[string]*MetaGroup
	sync.Mutex
}

func NewNameSpace(name string) (ns *NameSpace) {
	return &NameSpace{Name: name, MetaGroups: make(map[string]*MetaGroup, 0)}
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
