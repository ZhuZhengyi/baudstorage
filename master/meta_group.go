package master

import (
	"fmt"
	"github.com/tiglabs/baudstorage/proto"
	"sync"
)

type MetaRange struct {
	start uint64
	end   uint64
	Addr  string
}

type MetaGroup struct {
	GroupID          string
	Start            uint64
	End              uint64
	Members          []*MetaRange
	replicaNum       uint8
	status           uint8
	PersistenceHosts []string
	sync.Mutex
}

func NewMetaRange(start, end uint64, addr string) (mr *MetaRange) {
	mr = &MetaRange{start: start, end: end, Addr: addr}
	return
}

func NewMetaGroup(start, end uint64) (mg *MetaGroup) {
	groupID := fmt.Sprintf("%v_%v", start, end)
	mg = &MetaGroup{GroupID: groupID, Start: start, End: end, Members: make([]*MetaRange, 0)}
	return
}

func (mg *MetaGroup) AddMember(mr *MetaRange) (err error) {

	return
}

func (mg *MetaGroup) SelectHosts(c *Cluster) (err error) {
	return
}

func (mg *MetaGroup) createRange() {
	for _, addr := range mg.PersistenceHosts {
		mg.Members = append(mg.Members, NewMetaRange(mg.Start, mg.End, addr))
	}
}

func (mg *MetaGroup) generateCreateMetaGroupTasks() (tasks []*proto.AdminTask) {
	tasks = make([]*proto.AdminTask, 0)
	for _, addr := range mg.PersistenceHosts {
		tasks = append(tasks, proto.NewAdminTask(OpCreateMetaGroup, addr, nil))
	}
	return
}
