package master

import (
	"sync"

	"fmt"
	"github.com/tiglabs/baudstorage/proto"
	"github.com/tiglabs/baudstorage/util/log"
	"time"
)

type MetaReplica struct {
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

type MetaPartition struct {
	PartitionID      uint64
	Start            uint64
	End              uint64
	Replicas         []*MetaReplica
	replicaNum       uint8
	status           uint8
	PersistenceHosts []string
	peers            []proto.Peer
	MissNodes        map[string]int64
	sync.Mutex
}

func NewMetaReplica(start, end, id uint64, addr string) (mr *MetaReplica) {
	mr = &MetaReplica{start: start, end: end, nodeId: id, Addr: addr}
	mr.ReportTime = time.Now().Unix()
	mr.Total = DefaultMetaPartitionMemSize
	return
}

func NewMetaPartition(partitionID, start, end uint64) (mp *MetaPartition) {
	mp = &MetaPartition{PartitionID: partitionID, Start: start, End: end}
	mp.Replicas = make([]*MetaReplica, 0)
	mp.status = MetaPartitionUnavailable
	return
}

func (mp *MetaPartition) AddReplica(mr *MetaReplica) {
	mp.Lock()
	defer mp.Unlock()
	for _, m := range mp.Replicas {
		if m.Addr == mr.Addr {
			return
		}
	}
	mp.Replicas = append(mp.Replicas, mr)
	return
}

func (mp *MetaPartition) RemoveReplica(mr *MetaReplica) {
	mp.Lock()
	defer mp.Unlock()
	var newReplicas []*MetaReplica
	for _, m := range mp.Replicas {
		if m.Addr == mr.Addr {
			continue
		}
		newReplicas = append(newReplicas, m)
	}
	mp.Replicas = newReplicas
	return
}

func (mp *MetaPartition) updateEnd() {
	for _, mr := range mp.Replicas {
		mr.end = mp.End
	}
}

func (mp *MetaPartition) getMetaReplica(addr string) (mr *MetaReplica, err error) {
	mp.Lock()
	defer mp.Unlock()
	for _, mr = range mp.Replicas {
		if mr.Addr == addr {
			return
		}
	}
	return nil, metaReplicaNotFound(addr)
}

func (mp *MetaPartition) checkAndRemoveMissMetaReplica(addr string) {
	if _, ok := mp.MissNodes[addr]; ok {
		delete(mp.MissNodes, addr)
	}
}

func (mp *MetaPartition) checkStatus(writeLog bool, replicaNum int) {
	mp.Lock()
	defer mp.Unlock()
	missedCount := 0
	for _, metaReplica := range mp.Replicas {
		metaReplica.checkStatus()
		if metaReplica.isMissed() {
			mp.addMissNode(metaReplica.Addr, metaReplica.ReportTime)
			missedCount++
		}
		mp.status = metaReplica.status & metaReplica.status
	}

	if missedCount < replicaNum/2 {
		mp.status = MetaPartitionReadOnly
	}

	if writeLog {
		log.LogInfo(fmt.Sprintf("action[checkStatus],id:%v,status:%v,replicaNum:%v",
			mp.PartitionID, mp.status, mp.replicaNum))
	}
}

func (mp *MetaPartition) checkThreshold(threshold float32, size uint64) (t *proto.AdminTask) {
	mr, err := mp.getLeaderMetaReplica()
	if err != nil {
		log.LogError(fmt.Sprintf("meta group %v no leader", mp.PartitionID))
		return
	}
	if float32(mr.Used/size) > threshold {
		t = mr.generateUpdateMetaReplicaTask(mp.PartitionID)
	}
	return
}

func (mp *MetaPartition) getLeaderMetaReplica() (mr *MetaReplica, err error) {
	for _, mr = range mp.Replicas {
		if mr.isLeader {
			return
		}
	}
	err = NoLeader
	return
}

func (mp *MetaPartition) addMissNode(addr string, lastReportTime int64) {
	if _, ok := mp.MissNodes[addr]; !ok {
		mp.MissNodes[addr] = lastReportTime
	}
}

func (mp *MetaPartition) checkReplicas() {
	if int(mp.replicaNum) != len(mp.PersistenceHosts) {
		orgReplicaNum := mp.replicaNum
		mp.replicaNum = (uint8)(len(mp.PersistenceHosts))
		mp.updateHosts()
		msg := fmt.Sprintf("meta PartitionID:%v orgReplicaNum:%v locations:%v",
			mp.PartitionID, orgReplicaNum, mp.PersistenceHosts)
		log.LogWarn(msg)
	}
}

func (mp *MetaPartition) deleteExcessReplication() (excessAddr string, t *proto.AdminTask, err error) {

	var leaderMr *MetaReplica
	for _, mr := range mp.Replicas {
		if mr.isLeader {
			leaderMr = mr
		}
		if !contains(mp.PersistenceHosts, mr.Addr) {
			excessAddr = mr.Addr
			err = MetaGroupReplicationExcessError
			break
		}
	}
	if leaderMr == nil {
		leaderMr, err = mp.getLeaderMetaReplica()
		if err != nil {
			return
		}
	}
	t = leaderMr.generateDeleteReplicaTask(mp.PartitionID)
	return
}

func (mp *MetaPartition) getLackReplication() (lackAddrs []string) {

	var liveReplicas []string
	for _, mr := range mp.Replicas {
		liveReplicas = append(liveReplicas, mr.Addr)
	}
	for _, host := range mp.PersistenceHosts {
		if !contains(liveReplicas, host) {
			lackAddrs = append(lackAddrs, host)
			break
		}
	}
	return
}

func (mp *MetaPartition) updateHosts() {
	//todo
}

func (mp *MetaPartition) updateMetaPartition(mgr *proto.MetaPartitionReport, metaNode *MetaNode) {
	mp.Lock()
	mr, err := mp.getMetaReplica(metaNode.Addr)
	mp.Unlock()

	if err != nil && !contains(mp.PersistenceHosts, metaNode.Addr) {
		return
	}

	if err != nil && contains(mp.PersistenceHosts, metaNode.Addr) {
		mr = NewMetaReplica(mp.Start, mp.End, metaNode.id, metaNode.Addr)
		mp.AddReplica(mr)
	}
	mr.status = (uint8)(mgr.Status)
	mr.Used = mgr.Used
	mr.isLeader = mgr.IsLeader
	mr.setLastReportTime()
	mp.Lock()
	mp.checkAndRemoveMissMetaReplica(metaNode.Addr)
	mp.Unlock()
}

func (mp *MetaPartition) generateReplicaTask() (tasks []*proto.AdminTask) {
	var msg string
	tasks = make([]*proto.AdminTask, 0)
	if excessAddr, task, excessErr := mp.deleteExcessReplication(); excessErr != nil {
		msg = fmt.Sprintf("action[%v], metaPartition:%v  excess replication"+
			" on :%v  err:%v  persistenceHosts:%v",
			DeleteExcessReplicationErr, mp.PartitionID, excessAddr, excessErr.Error(), mp.PersistenceHosts)
		log.LogWarn(msg)
		tasks = append(tasks, task)
	}
	if lackAddrs := mp.getLackReplication(); lackAddrs == nil {
		msg = fmt.Sprintf("action[getLackReplication], metaPartition:%v  lack replication"+
			" on :%v PersistenceHosts:%v",
			mp.PartitionID, lackAddrs, mp.PersistenceHosts)
		log.LogWarn(msg)
		tasks = append(tasks, mp.generateAddLackMetaReplicaTask(lackAddrs)...)
	}

	return
}

func (mp *MetaPartition) generateCreateMetaPartitionTasks(specifyAddrs []string) (tasks []*proto.AdminTask) {
	tasks = make([]*proto.AdminTask, 0)
	hosts := make([]string, 0)
	req := &proto.CreateMetaPartitionRequest{
		Start:   mp.Start,
		End:     mp.End,
		GroupId: mp.PartitionID,
		Members: mp.peers,
	}
	if specifyAddrs == nil {
		hosts = mp.PersistenceHosts
	} else {
		hosts = specifyAddrs
	}

	for _, addr := range hosts {
		tasks = append(tasks, proto.NewAdminTask(OpCreateMetaPartition, addr, req))
	}

	return
}

func (mp *MetaPartition) generateAddLackMetaReplicaTask(addrs []string) (tasks []*proto.AdminTask) {
	return mp.generateCreateMetaPartitionTasks(addrs)
}

func (mp *MetaPartition) generateLoadMetaPartitionTasks() (tasks []*proto.AdminTask) {
	req := &proto.LoadMetaPartitionMetricRequest{PartitionID: mp.PartitionID}
	for _, mr := range mp.Replicas {
		t := proto.NewAdminTask(OpLoadMetaPartition, mr.Addr, req)
		tasks = append(tasks, t)
	}

	return
}

func (mr *MetaReplica) generateUpdateMetaReplicaTask(groupId uint64) (t *proto.AdminTask) {
	req := &proto.DeleteMetaPartitionRequest{GroupId: groupId}
	t = proto.NewAdminTask(OpUpdateMetaPartition, mr.Addr, req)
	return
}

func (mr *MetaReplica) generateDeleteReplicaTask(groupId uint64) (t *proto.AdminTask) {
	req := &proto.DeleteMetaPartitionRequest{GroupId: groupId}
	t = proto.NewAdminTask(OpDeleteMetaPartition, mr.Addr, req)
	return
}

func (mr *MetaReplica) checkStatus() {
	if time.Now().Unix()-mr.ReportTime > DefaultMetaPartitionTimeOutSec {
		mr.status = MetaPartitionUnavailable
	}
}

func (mr *MetaReplica) setStatus(status uint8) {
	mr.status = status
}

func (mr *MetaReplica) isMissed() (miss bool) {
	return time.Now().Unix()-mr.ReportTime > DefaultMetaPartitionTimeOutSec
}

func (mr *MetaReplica) setLastReportTime() {
	mr.ReportTime = time.Now().Unix()
}
