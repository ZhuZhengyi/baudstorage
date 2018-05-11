package master

import (
	"sync"

	"github.com/tiglabs/baudstorage/proto"
)

type MetaRange struct {
	Addr   string
	start  uint64
	end    uint64
	id     uint64
	status uint8
}

type MetaGroup struct {
	GroupID          uint64
	Start            uint64
	End              uint64
	Members          []*MetaRange
	replicaNum       uint8
	status           uint8
	PersistenceHosts []string
	MissNodes        map[string]int64
	sync.Mutex
}

func NewMetaRange(start, end, id uint64, addr string) (mr *MetaRange) {
	mr = &MetaRange{start: start, end: end, id: id, Addr: addr}
	return
}

func NewMetaGroup(groupId, start, end uint64) (mg *MetaGroup) {
	mg = &MetaGroup{GroupID: groupId, Start: start, End: end, Members: make([]*MetaRange, 0)}
	return
}

func (mg *MetaGroup) AddMember(mr *MetaRange) {
	mg.Lock()
	defer mg.Unlock()
	for _, m := range mg.Members {
		if m.Addr == mr.Addr {
			return
		}
	}
	mg.Members = append(mg.Members, mr)
	return
}

func (mg *MetaGroup) checkAndRemoveMissMetaRange(addr string) {
	if _, ok := mg.MissNodes[addr]; ok {
		delete(mg.MissNodes, addr)
	}
}

func (mg *MetaGroup) generateCreateMetaGroupTasks(nsName string) (tasks []*proto.AdminTask) {
	tasks = make([]*proto.AdminTask, 0)
	peers := make([]proto.Peer, 0)
	for _, m := range mg.Members {
		peer := proto.Peer{ID: m.id, Addr: m.Addr}
		peers = append(peers, peer)

	}
	req := &proto.CreateMetaRangeRequest{
		Start:   mg.Start,
		End:     mg.End,
		GroupId: mg.GroupID,
		Members: peers,
	}
	for _, addr := range mg.PersistenceHosts {
		tasks = append(tasks, proto.NewAdminTask(OpCreateMetaGroup, addr, req))
	}
	return
}
