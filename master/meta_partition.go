package master

import (
	"sync"

	"fmt"
	"github.com/juju/errors"
	"github.com/tiglabs/baudstorage/proto"
	"github.com/tiglabs/baudstorage/util/log"
	"strings"
	"time"
)

type MetaReplica struct {
	Addr       string
	start      uint64
	end        uint64
	nodeId     uint64
	ReportTime int64
	Status     uint8
	IsLeader   bool
	metaNode   *MetaNode
}

type MetaPartition struct {
	PartitionID      uint64
	Start            uint64
	End              uint64
	MaxNodeID        uint64
	Replicas         []*MetaReplica
	CurReplicaNum    uint8
	Status           uint8
	nsName           string
	PersistenceHosts []string
	Peers            []proto.Peer
	MissNodes        map[string]int64
	sync.Mutex
}

func NewMetaReplica(start, end uint64, metaNode *MetaNode) (mr *MetaReplica) {
	mr = &MetaReplica{start: start, end: end, nodeId: metaNode.ID, Addr: metaNode.Addr}
	mr.metaNode = metaNode
	mr.ReportTime = time.Now().Unix()
	return
}

func NewMetaPartition(partitionID, start, end uint64, nsName string) (mp *MetaPartition) {
	mp = &MetaPartition{PartitionID: partitionID, Start: start, End: end, nsName: nsName}
	mp.Replicas = make([]*MetaReplica, 0)
	mp.Status = MetaPartitionUnavailable
	mp.MissNodes = make(map[string]int64, 0)
	mp.Peers = make([]proto.Peer, 0)
	mp.PersistenceHosts = make([]string, 0)
	return
}

func (mp *MetaPartition) setPeers(peers []proto.Peer) {
	mp.Peers = peers
}

func (mp *MetaPartition) setPersistenceHosts(hosts []string) {
	mp.PersistenceHosts = hosts
}

func (mp *MetaPartition) hostsToString() (hosts string) {
	return strings.Join(mp.PersistenceHosts, UnderlineSeparator)
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

func (mp *MetaPartition) RemoveReplicaByAddr(addr string) {
	mp.Lock()
	defer mp.Unlock()
	var newReplicas []*MetaReplica
	for _, m := range mp.Replicas {
		if m.Addr == addr {
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
	mp.Lock()
	defer mp.Unlock()
	if _, ok := mp.MissNodes[addr]; ok {
		delete(mp.MissNodes, addr)
	}
}

func (mp *MetaPartition) checkStatus(writeLog bool, replicaNum int) {
	mp.Lock()
	defer mp.Unlock()
	missedCount := 0
	for _, metaReplica := range mp.Replicas {
		metaReplica.checkStatus(mp.MaxNodeID)
		if metaReplica.isMissed() {
			mp.addMissNode(metaReplica.Addr, metaReplica.ReportTime)
			missedCount++
		}
		mp.Status = mp.Status & metaReplica.Status
	}

	if mp.Status == MetaPartitionUnavailable {
		mp.Status = MetaPartitionReadOnly
	}

	if missedCount < replicaNum/2 {
		mp.Status = MetaPartitionReadOnly
	}

	if writeLog {
		log.LogInfo(fmt.Sprintf("action[checkStatus],id:%v,status:%v,replicaNum:%v,missNodes:%v",
			mp.PartitionID, mp.Status, mp.CurReplicaNum, mp.MissNodes))
	}
}

func (mp *MetaPartition) getLeaderMetaReplica() (mr *MetaReplica, err error) {
	for _, mr = range mp.Replicas {
		if mr.IsLeader {
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

func (mp *MetaPartition) checkReplicas(c *Cluster, nsName string, replicaNum uint8) {
	mp.Lock()
	defer mp.Unlock()
	if int(mp.CurReplicaNum) != len(mp.PersistenceHosts) {
		orgReplicaNum := mp.CurReplicaNum
		mp.CurReplicaNum = (uint8)(len(mp.PersistenceHosts))
		mp.updateHosts(c, nsName)
		msg := fmt.Sprintf("meta PartitionID:%v orgReplicaNum:%v locations:%v",
			mp.PartitionID, orgReplicaNum, mp.PersistenceHosts)
		log.LogWarn(msg)
	}

	if mp.CurReplicaNum != replicaNum {
		msg := fmt.Sprintf("namespace replica num[%v],meta partition current num[%v]", replicaNum, mp.CurReplicaNum)
		log.LogWarn(msg)
	}
}

func (mp *MetaPartition) getRacks(excludeAddr string) (racks []string) {
	racks = make([]string, 0)
	for _, mr := range mp.Replicas {
		if mr.Addr != excludeAddr {
			racks = append(racks, excludeAddr)
		}
	}
	return
}

func (mp *MetaPartition) deleteExcessReplication() (excessAddr string, t *proto.AdminTask, err error) {

	for _, mr := range mp.Replicas {
		if !contains(mp.PersistenceHosts, mr.Addr) {
			t = mr.generateDeleteReplicaTask(mp.PartitionID)
			err = MetaReplicaExcessError
			break
		}
	}
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

func (mp *MetaPartition) updateHosts(c *Cluster, nsName string) (err error) {
	return c.syncUpdateMetaPartition(nsName, mp)
}

func (mp *MetaPartition) updateMetaPartition(mgr *proto.MetaPartitionReport, metaNode *MetaNode) {
	mr, err := mp.getMetaReplica(metaNode.Addr)
	if err != nil && !contains(mp.PersistenceHosts, metaNode.Addr) {
		return
	}

	if err != nil && contains(mp.PersistenceHosts, metaNode.Addr) {
		mr = NewMetaReplica(mp.Start, mp.End, metaNode)
		mp.AddReplica(mr)
	}
	mr.updateMetric(mgr)
	mp.checkAndRemoveMissMetaReplica(metaNode.Addr)
}

func (mp *MetaPartition) canOffline(nodeAddr string, replicaNum int) (err error) {
	liveReplicas := mp.getLiveReplica()
	if !mp.hasMajorityReplicas(len(liveReplicas), replicaNum) {
		err = NoHaveMajorityReplica
		return
	}
	liveAddrs := mp.getLiveReplicasAddr(liveReplicas)
	if len(liveReplicas) == (replicaNum/2+1) && contains(liveAddrs, nodeAddr) {
		err = fmt.Errorf("live replicas num will be less than majority after offline nodeAddr: %v", nodeAddr)
		return
	}
	return
}

func (mp *MetaPartition) hasMajorityReplicas(liveReplicas int, replicaNum int) bool {
	return liveReplicas >= int(mp.CurReplicaNum/2+1)
}

func (mp *MetaPartition) getLiveReplicasAddr(liveReplicas []*MetaReplica) (addrs []string) {
	addrs = make([]string, 0)
	for _, mr := range liveReplicas {
		addrs = append(addrs, mr.Addr)
	}
	return
}
func (mp *MetaPartition) getLiveReplica() (liveReplicas []*MetaReplica) {
	liveReplicas = make([]*MetaReplica, 0)
	for _, mr := range mp.Replicas {
		if mr.isActive() {
			liveReplicas = append(liveReplicas, mr)
		}
	}
	return
}

func (mp *MetaPartition) updateInfoToStore(newHosts []string, newPeers []proto.Peer, nsName string, c *Cluster) (err error) {
	oldVolHosts := make([]string, len(mp.PersistenceHosts))
	copy(oldVolHosts, mp.PersistenceHosts)
	oldPeers := make([]proto.Peer, len(mp.Peers))
	copy(oldPeers, mp.Peers)
	mp.PersistenceHosts = newHosts
	mp.Peers = newPeers
	if err = c.syncUpdateMetaPartition(nsName, mp); err != nil {
		mp.PersistenceHosts = oldVolHosts
		mp.Peers = oldPeers
		log.LogWarnf("action[updateInfoToStore] failed,partitionID:%v  old hosts:%v new hosts:%v oldPeers:%v  newPeers",
			mp.PartitionID, mp.PersistenceHosts, newHosts, mp.Peers, newPeers)
	}
	log.LogDebugf("action[updateInfoToStore] success,partitionID:%v  old hosts:%v  new hosts:%v ",
		mp.PartitionID, oldVolHosts, mp.PersistenceHosts, oldPeers, mp.Peers)
	return
}

func (mp *MetaPartition) getLiveAddrs() (liveAddrs []string) {
	liveAddrs = make([]string, 0)
	for _, mr := range mp.Replicas {
		if mr.isActive() {
			liveAddrs = append(liveAddrs, mr.Addr)
		}
	}
	return liveAddrs
}

func (mp *MetaPartition) missedReplica(addr string) bool {
	return !contains(mp.getLiveAddrs(), addr)
}

func (mp *MetaPartition) needWarnMissReplica(addr string, warnInterval int64) (isWarn bool) {
	lastWarnTime, ok := mp.MissNodes[addr]
	if !ok {
		isWarn = true
		mp.MissNodes[addr] = time.Now().Unix()
	} else if (time.Now().Unix() - lastWarnTime) > warnInterval {
		isWarn = true
		mp.MissNodes[addr] = time.Now().Unix()
	}
	return false
}

func (mp *MetaPartition) checkReplicaMiss(clusterID string, partitionMissSec, warnInterval int64) {
	mp.Lock()
	defer mp.Unlock()
	//has report
	for _, replica := range mp.Replicas {
		if contains(mp.PersistenceHosts, replica.Addr) && replica.isMissed() == true && mp.needWarnMissReplica(replica.Addr, warnInterval) {
			metaNode := replica.metaNode
			var (
				lastReportTime time.Time
			)
			isActive := true
			if metaNode != nil {
				lastReportTime = metaNode.ReportTime
				isActive = metaNode.IsActive
			}
			msg := fmt.Sprintf("action[checkReplicaMiss], partition:%v  on Node:%v  "+
				"miss time > :%v  vlocLastRepostTime:%v   dnodeLastReportTime:%v  nodeisActive:%v So Migrate", mp.PartitionID,
				replica.Addr, partitionMissSec, replica.ReportTime, lastReportTime, isActive)
			Warn(clusterID, msg)
		}
	}
	// never report
	for _, addr := range mp.PersistenceHosts {
		if mp.missedReplica(addr) && mp.needWarnMissReplica(addr, warnInterval) {
			msg := fmt.Sprintf("action[checkReplicaMiss], partition:%v  on Node:%v  "+
				"miss time  > :%v  but server not exsit So Migrate", mp.PartitionID, addr, DefaultMetaPartitionTimeOutSec)
			Warn(clusterID, msg)
		}
	}
}

func (mp *MetaPartition) generateReplicaTask(nsName string) (tasks []*proto.AdminTask) {
	var msg string
	tasks = make([]*proto.AdminTask, 0)
	if excessAddr, task, excessErr := mp.deleteExcessReplication(); excessErr != nil {
		msg = fmt.Sprintf("action[%v], metaPartition:%v  excess replication"+
			" on :%v  err:%v  persistenceHosts:%v",
			DeleteExcessReplicationErr, mp.PartitionID, excessAddr, excessErr.Error(), mp.PersistenceHosts)
		log.LogWarn(msg)
		tasks = append(tasks, task)
	}
	if lackAddrs := mp.getLackReplication(); lackAddrs != nil {
		msg = fmt.Sprintf("action[getLackReplication], metaPartition:%v  lack replication"+
			" on :%v PersistenceHosts:%v",
			mp.PartitionID, lackAddrs, mp.PersistenceHosts)
		log.LogWarn(msg)
		tasks = append(tasks, mp.generateAddLackMetaReplicaTask(lackAddrs, nsName)...)
	}

	return
}

func (mp *MetaPartition) generateCreateMetaPartitionTasks(specifyAddrs []string, nsName string) (tasks []*proto.AdminTask) {
	tasks = make([]*proto.AdminTask, 0)
	hosts := make([]string, 0)
	req := &proto.CreateMetaPartitionRequest{
		Start:       mp.Start,
		End:         mp.End,
		PartitionID: mp.PartitionID,
		Members:     mp.Peers,
		NsName:      nsName,
	}
	if specifyAddrs == nil {
		hosts = mp.PersistenceHosts
	} else {
		hosts = specifyAddrs
	}

	for _, addr := range hosts {
		t := proto.NewAdminTask(proto.OpCreateMetaPartition, addr, req)
		t.ID = fmt.Sprintf("%v_pid[%v]", t.ID, mp.PartitionID)
		tasks = append(tasks, t)
	}
	return
}

func (mp *MetaPartition) generateAddLackMetaReplicaTask(addrs []string, nsName string) (tasks []*proto.AdminTask) {
	return mp.generateCreateMetaPartitionTasks(addrs, nsName)
}

func (mp *MetaPartition) generateOfflineTask(nsName string, removePeer proto.Peer, addPeer proto.Peer) (t *proto.AdminTask, err error) {
	mr, err := mp.getLeaderMetaReplica()
	if err != nil {
		return nil, errors.Trace(err)
	}
	req := &proto.MetaPartitionOfflineRequest{PartitionID: mp.PartitionID, NsName: nsName, RemovePeer: removePeer, AddPeer: addPeer}
	t = proto.NewAdminTask(proto.OpOfflineMetaPartition, mr.Addr, req)
	t.ID = fmt.Sprintf("%v_pid[%v]", t.ID, mp.PartitionID)
	return
}

func (mp *MetaPartition) generateLoadMetaPartitionTasks() (tasks []*proto.AdminTask) {
	req := &proto.LoadMetaPartitionMetricRequest{PartitionID: mp.PartitionID}
	for _, mr := range mp.Replicas {
		t := proto.NewAdminTask(proto.OpLoadMetaPartition, mr.Addr, req)
		t.ID = fmt.Sprintf("%v_pid[%v]", t.ID, mp.PartitionID)
		tasks = append(tasks, t)
	}

	return
}

func (mp *MetaPartition) generateUpdateMetaReplicaTask(partitionID uint64, end uint64) (t *proto.AdminTask) {
	mr, err := mp.getLeaderMetaReplica()
	if err != nil {
		log.LogError(fmt.Sprintf("meta group %v no leader", mp.PartitionID))
		return
	}
	req := &proto.UpdateMetaPartitionRequest{PartitionID: partitionID, End: end, NsName: mp.nsName}
	t = proto.NewAdminTask(proto.OpUpdateMetaPartition, mr.Addr, req)
	t.ID = fmt.Sprintf("%v_pid[%v]", t.ID, mp.PartitionID)
	return
}

func (mr *MetaReplica) generateDeleteReplicaTask(partitionID uint64) (t *proto.AdminTask) {
	req := &proto.DeleteMetaPartitionRequest{PartitionID: partitionID}
	t = proto.NewAdminTask(proto.OpDeleteMetaPartition, mr.Addr, req)
	t.ID = fmt.Sprintf("%v_pid[%v]", t.ID, partitionID)
	return
}

func (mr *MetaReplica) checkStatus(maxInodeID uint64) {
	if time.Now().Unix()-mr.ReportTime > DefaultMetaPartitionTimeOutSec {
		mr.Status = MetaPartitionUnavailable
	}
	if maxInodeID < mr.end {
		mr.Status = MetaPartitionReadWrite
	} else {
		mr.Status = MetaPartitionReadOnly
	}

}

func (mr *MetaReplica) setStatus(status uint8) {
	mr.Status = status
}

func (mr *MetaReplica) isMissed() (miss bool) {
	return time.Now().Unix()-mr.ReportTime > DefaultMetaPartitionTimeOutSec
}

func (mr *MetaReplica) isActive() (active bool) {
	return time.Now().Unix()-mr.ReportTime < DefaultMetaPartitionTimeOutSec
}

func (mr *MetaReplica) setLastReportTime() {
	mr.ReportTime = time.Now().Unix()
}

func (mr *MetaReplica) updateMetric(mgr *proto.MetaPartitionReport) {
	mr.Status = (uint8)(mgr.Status)
	mr.IsLeader = mgr.IsLeader
	mr.setLastReportTime()
}
