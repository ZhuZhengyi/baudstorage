package master

import (
	"sync"

	"fmt"
	"github.com/tiglabs/baudstorage/proto"
	"github.com/tiglabs/baudstorage/util/log"
	"time"
)

type MetaRange struct {
	Addr       string
	start      uint64
	end        uint64
	nodeId     uint64
	ReportTime int64
	status     uint8
	isLeader   bool
	Total      uint64 `json:"TotalSize"`
	Used       uint64 `json:"UsedSize"`
}

type MetaGroup struct {
	GroupID          uint64
	Start            uint64
	End              uint64
	Members          []*MetaRange
	replicaNum       uint8
	status           uint8
	PersistenceHosts []string
	peers            []proto.Peer
	MissNodes        map[string]int64
	sync.Mutex
}

func NewMetaRange(start, end, id uint64, addr string) (mr *MetaRange) {
	mr = &MetaRange{start: start, end: end, nodeId: id, Addr: addr}
	mr.ReportTime = time.Now().Unix()
	mr.Total = DefaultMetaRangeMemSize
	return
}

func NewMetaGroup(groupId, start, end uint64) (mg *MetaGroup) {
	mg = &MetaGroup{GroupID: groupId, Start: start, End: end}
	mg.Members = make([]*MetaRange, 0)
	mg.status = MetaRangeUnavailable
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

func (mg *MetaGroup) RemoveMember(mr *MetaRange) {
	mg.Lock()
	defer mg.Unlock()
	var newMembers []*MetaRange
	for _, m := range mg.Members {
		if m.Addr == mr.Addr {
			continue
		}
		newMembers = append(newMembers, m)
	}
	mg.Members = newMembers
	return
}

func (mg *MetaGroup) updateEnd() {
	for _, mr := range mg.Members {
		mr.end = mg.End
	}
}

func (mg *MetaGroup) getMetaRange(addr string) (mr *MetaRange, err error) {
	mg.Lock()
	defer mg.Unlock()
	for _, mr = range mg.Members {
		if mr.Addr == addr {
			return
		}
	}
	return nil, metaRangeNotFound(addr)
}

func (mg *MetaGroup) checkAndRemoveMissMetaRange(addr string) {
	if _, ok := mg.MissNodes[addr]; ok {
		delete(mg.MissNodes, addr)
	}
}

func (mg *MetaGroup) checkStatus(writeLog bool, replicaNum int) {
	mg.Lock()
	defer mg.Unlock()
	missedCount := 0
	for _, metaRange := range mg.Members {
		metaRange.checkStatus()
		if metaRange.isMissed() {
			mg.addMissNode(metaRange.Addr, metaRange.ReportTime)
			missedCount++
		}
		mg.status = metaRange.status & metaRange.status
	}

	if missedCount < replicaNum/2 {
		mg.status = MetaRangeReadOnly
	}

	if writeLog {
		log.LogInfo(fmt.Sprintf("action[checkStatus],id:%v,status:%v,replicaNum:%v",
			mg.GroupID, mg.status, mg.replicaNum))
	}
}

func (mg *MetaGroup) checkThreshold(threshold float32, size uint64) (t *proto.AdminTask) {
	mr, err := mg.getLeaderMetaRange()
	if err != nil {
		log.LogError(fmt.Sprintf("meta group %v no leader", mg.GroupID))
		return
	}
	if float32(mr.Used/size) > threshold {
		t = mr.generateUpdateMetaRangeTask(mg.GroupID)
	}
	return
}

func (mg *MetaGroup) getLeaderMetaRange() (mr *MetaRange, err error) {
	for _, mr = range mg.Members {
		if mr.isLeader {
			return
		}
	}
	err = NoLeader
	return
}

func (mg *MetaGroup) addMissNode(addr string, lastReportTime int64) {
	if _, ok := mg.MissNodes[addr]; !ok {
		mg.MissNodes[addr] = lastReportTime
	}
}

func (mg *MetaGroup) checkReplicas() {
	if int(mg.replicaNum) != len(mg.PersistenceHosts) {
		orgReplicaNum := mg.replicaNum
		mg.replicaNum = (uint8)(len(mg.PersistenceHosts))
		mg.updateHosts()
		msg := fmt.Sprintf("metaGroupId:%v orgReplicaNum:%v locations:%v",
			mg.GroupID, orgReplicaNum, mg.PersistenceHosts)
		log.LogWarn(msg)
	}
}

func (mg *MetaGroup) deleteExcessReplication() (excessAddr string, t *proto.AdminTask, err error) {

	var leaderMr *MetaRange
	for _, mr := range mg.Members {
		if mr.isLeader {
			leaderMr = mr
		}
		if !contains(mg.PersistenceHosts, mr.Addr) {
			excessAddr = mr.Addr
			err = MetaGroupReplicationExcessError
			break
		}
	}
	if leaderMr == nil {
		leaderMr, err = mg.getLeaderMetaRange()
		if err != nil {
			return
		}
	}
	t = leaderMr.generateDeleteReplicaTask(mg.GroupID)
	return
}

func (mg *MetaGroup) getLackReplication() (lackAddrs []string) {

	var liveReplicas []string
	for _, mr := range mg.Members {
		liveReplicas = append(liveReplicas, mr.Addr)
	}
	for _, host := range mg.PersistenceHosts {
		if !contains(liveReplicas, host) {
			lackAddrs = append(lackAddrs, host)
			break
		}
	}
	return
}

func (mg *MetaGroup) updateHosts() {
	//todo
}

func (mg *MetaGroup) updateMetaGroup(mgr *proto.MetaRangeReport, metaNode *MetaNode) {
	mg.Lock()
	mr, err := mg.getMetaRange(metaNode.Addr)
	mg.Unlock()

	if err != nil && !contains(mg.PersistenceHosts, metaNode.Addr) {
		return
	}

	if err != nil && contains(mg.PersistenceHosts, metaNode.Addr) {
		mr = NewMetaRange(mg.Start, mg.End, metaNode.id, metaNode.Addr)
		mg.AddMember(mr)
	}
	mr.status = (uint8)(mgr.Status)
	mr.Used = mgr.Used
	mr.isLeader = mgr.IsLeader
	mr.setLastReportTime()
	mg.Lock()
	mg.checkAndRemoveMissMetaRange(metaNode.Addr)
	mg.Unlock()
}

func (mg *MetaGroup) generateReplicaTask() (tasks []*proto.AdminTask) {
	var msg string
	tasks = make([]*proto.AdminTask, 0)
	if excessAddr, task, excessErr := mg.deleteExcessReplication(); excessErr != nil {
		msg = fmt.Sprintf("action[%v], metaGroup:%v  excess replication"+
			" on :%v  err:%v  persistenceHosts:%v",
			DeleteExcessReplicationErr, mg.GroupID, excessAddr, excessErr.Error(), mg.PersistenceHosts)
		log.LogWarn(msg)
		tasks = append(tasks, task)
	}
	if lackAddrs := mg.getLackReplication(); lackAddrs == nil {
		msg = fmt.Sprintf("action[getLackReplication], metaGroupId:%v  lack replication"+
			" on :%v PersistenceHosts:%v",
			mg.GroupID, lackAddrs, mg.PersistenceHosts)
		log.LogWarn(msg)
		tasks = append(tasks, mg.generateAddLackMetaRangeTask(lackAddrs)...)
	}

	return
}

func (mg *MetaGroup) generateCreateMetaGroupTasks(specifyAddrs []string) (tasks []*proto.AdminTask) {
	tasks = make([]*proto.AdminTask, 0)
	hosts := make([]string, 0)
	req := &proto.CreateMetaRangeRequest{
		Start:   mg.Start,
		End:     mg.End,
		GroupId: mg.GroupID,
		Members: mg.peers,
	}
	if specifyAddrs == nil {
		hosts = mg.PersistenceHosts
	} else {
		hosts = specifyAddrs
	}

	for _, addr := range hosts {
		tasks = append(tasks, proto.NewAdminTask(OpCreateMetaGroup, addr, req))
	}

	return
}

func (mg *MetaGroup) generateAddLackMetaRangeTask(addrs []string) (tasks []*proto.AdminTask) {
	return mg.generateCreateMetaGroupTasks(addrs)
}

func (mr *MetaRange) generateUpdateMetaRangeTask(groupId uint64) (t *proto.AdminTask) {
	req := &proto.DeleteMetaRangeRequest{GroupId: groupId}
	t = proto.NewAdminTask(OpUpdateMetaRange, mr.Addr, req)
	return
}

func (mr *MetaRange) generateDeleteReplicaTask(groupId uint64) (t *proto.AdminTask) {
	req := &proto.DeleteMetaRangeRequest{GroupId: groupId}
	t = proto.NewAdminTask(OpDeleteMetaRange, mr.Addr, req)
	return
}

func (mr *MetaRange) checkStatus() {
	if time.Now().Unix()-mr.ReportTime > DefaultMetaRangeTimeOutSec {
		mr.status = MetaRangeUnavailable
	}
}

func (mr *MetaRange) setStatus(status uint8) {
	mr.status = status
}

func (mr *MetaRange) isMissed() (miss bool) {
	return time.Now().Unix()-mr.ReportTime > DefaultMetaRangeTimeOutSec
}

func (mr *MetaRange) setLastReportTime() {
	mr.ReportTime = time.Now().Unix()
}
