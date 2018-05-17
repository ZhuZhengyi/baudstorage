package master

import (
	"fmt"
	"github.com/tiglabs/baudstorage/proto"
	"github.com/tiglabs/baudstorage/util/log"
	"runtime"
	"sync"
	"time"
)

func (c *Cluster) putDataNodeTasks(tasks []*proto.AdminTask) {

	for _, t := range tasks {
		if t == nil {
			continue
		}
		if node, err := c.getDataNode(t.OperatorAddr); err != nil {
			log.LogWarn(fmt.Sprintf("action[putTasks],nodeAddr:%v,taskID:%v,err:%v", node.HttpAddr, t.ID, err.Error()))
		} else {
			node.sender.PutTask(t)
		}
	}
}

func (c *Cluster) putMetaNodeTasks(tasks []*proto.AdminTask) {

	for _, t := range tasks {
		if t == nil {
			continue
		}
		if node, err := c.getMetaNode(t.OperatorAddr); err != nil {
			log.LogWarn(fmt.Sprintf("action[putTasks],nodeAddr:%v,taskID:%v,err:%v", node.Addr, t.ID, err.Error()))
		} else {
			node.sender.PutTask(t)

		}
	}
}

func (c *Cluster) checkVolGroups(ns *NameSpace) {
	ns.volGroups.RLock()
	newReadWriteVolGroups := 0
	for _, vg := range ns.volGroups.volGroupMap {
		vg.checkLocationStatus(c.cfg.VolTimeOutSec)
		vg.checkStatus(true, c.cfg.VolTimeOutSec)
		vg.checkVolGroupMiss(c.cfg.VolMissSec, c.cfg.VolWarnInterval)
		vg.checkReplicaNum(c, ns.Name)
		if vg.status == VolReadWrite {
			newReadWriteVolGroups++
		}
		volDiskErrorAddrs := vg.checkVolDiskError()
		if volDiskErrorAddrs != nil {
			for _, addr := range volDiskErrorAddrs {
				c.volOffline(addr, ns.Name, vg, CheckVolDiskErrorErr)
			}
		}
		volTasks := vg.checkVolReplicationTask()
		c.putDataNodeTasks(volTasks)
	}
	ns.volGroups.readWriteVolGroups = newReadWriteVolGroups
	ns.volGroups.RUnlock()
	ns.volGroups.updateVolResponseCache(NeedUpdateVolResponse, 0)
	msg := fmt.Sprintf("action[CheckVolInfo],can readwrite volGroups:%v  ", ns.volGroups.readWriteVolGroups)
	log.LogInfo(msg)
}

func (c *Cluster) backendLoadVolGroup(ns *NameSpace) {
	needCheckVols := ns.volGroups.getNeedCheckVolGroups(c.cfg.everyLoadVolCount, c.cfg.LoadVolFrequencyTime)
	if len(needCheckVols) == 0 {
		return
	}
	c.waitLoadVolResponse(needCheckVols)
	msg := fmt.Sprintf("action[BackendLoadVol] checkstart:%v everyCheckCount:%v",
		needCheckVols[0].VolID, c.cfg.everyLoadVolCount)
	log.LogInfo(msg)
}

func (c *Cluster) waitLoadVolResponse(needCheckVols []*VolGroup) {

	defer func() {
		if err := recover(); err != nil {
			const size = RuntimeStackBufSize
			buf := make([]byte, size)
			buf = buf[:runtime.Stack(buf, false)]
			log.LogError(fmt.Sprintf("waitLoadVolResponse panic %v: %s\n", err, buf))
		}
	}()
	var wg sync.WaitGroup
	for _, v := range needCheckVols {
		wg.Add(1)
		go func(v *VolGroup) {
			c.processLoadVol(v, false)
			wg.Done()
		}(v)
	}
	wg.Wait()
}

func (c *Cluster) processReleaseVolAfterLoadVolGroup(ns *NameSpace) {
	needReleaseVols := ns.volGroups.getNeedReleaseVolGroups(c.cfg.everyReleaseVolCount, c.cfg.releaseVolAfterLoadVolSeconds)
	if len(needReleaseVols) == 0 {
		return
	}
	ns.volGroups.releaseVolGroups(needReleaseVols)
	msg := fmt.Sprintf("action[processReleaseVolAfterLoadVolGroup]  release vol start:%v everyReleaseVolCount:%v",
		needReleaseVols[0].VolID, c.cfg.everyReleaseVolCount)
	log.LogInfo(msg)
}

func (c *Cluster) loadVolAndCheckResponse(v *VolGroup, isRecover bool) {
	go func() {
		c.processLoadVol(v, isRecover)
	}()
}

func (c *Cluster) metaPartitionOffline(nsName, nodeAddr string, partitionID uint64) (err error) {
	var (
		ns         *NameSpace
		mp         *MetaPartition
		t          *proto.AdminTask
		tasks      []*proto.AdminTask
		racks      []string
		hosts      []string
		newHosts   []string
		peers      []proto.Peer
		removePeer proto.Peer
	)
	if ns, err = c.getNamespace(nsName); err != nil {
		goto errDeal
	}
	if mp, err = ns.getMetaPartitionById(partitionID); err != nil {
		goto errDeal
	}

	if !contains(mp.PersistenceHosts, nodeAddr) {
		return
	}

	if err = mp.canOffline(nodeAddr); err != nil {
		goto errDeal
	}

	racks = mp.getRacks(nodeAddr)
	hosts = make([]string, 0)
	hosts = append(hosts, nodeAddr)
	if newHosts, peers, err = c.getAvailMetaNodeHosts(racks[0], hosts, 1); err != nil {
		goto errDeal
	}

	for _, mr := range mp.Replicas {
		if mr.Addr == nodeAddr {
			removePeer = proto.Peer{ID: mr.nodeId, Addr: mr.Addr}
		} else {
			peers = append(peers, proto.Peer{ID: mr.nodeId, Addr: mr.Addr})
		}
	}
	mp.peers = peers
	tasks = mp.generateCreateMetaPartitionTasks(newHosts)
	if t, err = mp.generateOfflineTask(nsName, removePeer, peers[0]); err != nil {
		goto errDeal
	}
	tasks = append(tasks, t)
	c.putMetaNodeTasks(tasks)
	return
errDeal:
	log.LogError(fmt.Sprintf("action[metaPartitionOffline],nsName: %v,partitionID: %v,err: %v", nsName, partitionID, err.Error()))
	return
}

func (c *Cluster) loadMetaPartitionAndCheckResponse(mp *MetaPartition, isRecover bool) {
	go func() {
		c.processLoadMetaPartition(mp, isRecover)
	}()
}

func (c *Cluster) processLoadMetaPartition(mp *MetaPartition, isRecover bool) {
	tasks := mp.generateLoadMetaPartitionTasks()
	c.putMetaNodeTasks(tasks)
	//todo check response
}

func (c *Cluster) processLoadVol(v *VolGroup, isRecover bool) {
	log.LogInfo(fmt.Sprintf("action[processLoadVol],volID:%v,isRecover:%v", v.VolID, isRecover))
	loadVolTasks := v.generateLoadVolTasks()
	c.putDataNodeTasks(loadVolTasks)
	for i := 0; i < LoadVolWaitTime; i++ {
		if v.checkLoadVolResponse(c.cfg.VolTimeOutSec) {
			log.LogInfo(fmt.Sprintf("action[%v] triger all replication,volID:%v ", "loadVolAndCheckResponse", v.VolID))
			break
		}
		time.Sleep(time.Second)
	}
	// response is time out
	if v.checkLoadVolResponse(c.cfg.VolTimeOutSec) == false {
		return
	}
	v.getFileCount()
	checkFileTasks := v.checkFile(isRecover)
	v.setVolToNormal()
	c.putDataNodeTasks(checkFileTasks)
}

func (c *Cluster) checkMetaGroups(ns *NameSpace) {
	ns.metaPartitionLock.RLock()
	defer ns.metaPartitionLock.RUnlock()
	var tasks []*proto.AdminTask
	for _, mp := range ns.MetaPartitions {
		mp.checkStatus(true, int(ns.mpReplicaNum))
		mp.checkReplicas(c, ns.Name)
		tasks = append(tasks, mp.generateReplicaTask()...)
		tasks = append(tasks, mp.checkThreshold(ns.threshold, ns.mpSize))
	}
	c.putMetaNodeTasks(tasks)

}

func (c *Cluster) dealMetaNodeTaskResponse(nodeAddr string, task *proto.AdminTask) {
	if task == nil {
		return
	}
	var (
		metaNode *MetaNode
		err      error
	)

	if metaNode, err = c.getMetaNode(nodeAddr); err != nil {
		goto errDeal
	}
	if _, ok := metaNode.sender.taskMap[task.ID]; !ok {
		err = taskNotFound(task.ID)
		goto errDeal
	}
	if err = UnmarshalTaskResponse(task); err != nil {
		goto errDeal
	}

	switch task.OpCode {
	case OpCreateMetaPartition:
		response := task.Response.(*proto.CreateMetaPartitionResponse)
		c.dealCreateMetaPartition(task.OperatorAddr, response)
	case OpMetaNodeHeartbeat:
		response := task.Response.(*proto.MetaNodeHeartbeatResponse)
		c.dealMetaNodeHeartbeat(task.OperatorAddr, response)
	case OpDeleteMetaPartition:
		response := task.Response.(*proto.DeleteMetaPartitionResponse)
		c.dealDeleteMetaPartition(task.OperatorAddr, response)
	case OpUpdateMetaPartition:
		response := task.Response.(*proto.UpdateMetaPartitionResponse)
		c.dealUpdateMetaPartition(task.OperatorAddr, response)
	case OpLoadMetaPartition:
		response := task.Response.(*proto.LoadMetaPartitionMetricResponse)
		c.dealLoadMetaPartition(task.OperatorAddr, response)
	case OpOfflineMetaPartition:
		response := task.Response.(*proto.MetaPartitionOfflineResponse)
		c.dealOfflineMetaPartition(task.OperatorAddr, response)
	default:
		log.LogError(fmt.Sprintf("unknown operate code %v", task.OpCode))
	}

	return

errDeal:
	log.LogError(fmt.Sprintf("action[dealMetaNodeTaskResponse],nodeAddr %v,taskId %v,err %v",
		nodeAddr, task.ID, err.Error()))
	return
}

func (c *Cluster) dealOfflineMetaPartition(nodeAddr string, resp *proto.MetaPartitionOfflineResponse) {
	if resp.Status == proto.CmdFailed {
		log.LogError(fmt.Sprintf("action[dealOfflineMetaPartition],nodeAddr %v offline meta partition failed,err %v", nodeAddr, resp.Result))
		return
	}
	var err error
	mp, err := c.getMetaPartitionByID(resp.PartitionID)
	if err != nil {
		goto errDeal
	}
	if err = mp.removePersistenceHosts(nodeAddr, c, resp.NsName); err != nil {
		goto errDeal
	}
	mp.RemoveReplicaByAddr(nodeAddr)
	mp.checkAndRemoveMissMetaReplica(nodeAddr)
	return
errDeal:
	log.LogError(fmt.Sprintf("dealOfflineMetaPartition err: %v", err.Error()))
	return
}

func (c *Cluster) dealLoadMetaPartition(nodeAddr string, resp *proto.LoadMetaPartitionMetricResponse) {

}

func (c *Cluster) dealUpdateMetaPartition(nodeAddr string, resp *proto.UpdateMetaPartitionResponse) {
	if resp.Status == proto.CmdFailed {
		log.LogError(fmt.Sprintf("action[dealUpdateMetaPartition],nodeAddr %v update meta range failed,err %v", nodeAddr, resp.Result))
		return
	}
	mp, err := c.getMetaPartitionByID(resp.GroupId)
	mp.End = resp.End
	mp.updateEnd()
	if err != nil {
		goto errDeal
	}
	if err = c.CreateMetaPartition(resp.NsName, mp.End, DefaultMaxMetaPartitionRange); err != nil {
		goto errDeal
	}
	return
errDeal:
	log.LogError(fmt.Sprintf("createVolSuccessTriggerOperatorErr %v", err))
	return
}

func (c *Cluster) dealDeleteMetaPartition(nodeAddr string, resp *proto.DeleteMetaPartitionResponse) {
	if resp.Status == proto.CmdFailed {
		log.LogError(fmt.Sprintf("action[dealDeleteMetaPartition],nodeAddr %v delete meta range failed,err %v", nodeAddr, resp.Result))
		return
	}
	var mr *MetaReplica
	mp, err := c.getMetaPartitionByID(resp.GroupId)
	if err != nil {
		goto errDeal
	}
	if mr, err = mp.getMetaReplica(nodeAddr); err != nil {
		goto errDeal
	}
	mp.RemoveReplica(mr)
	return

errDeal:
	log.LogError(fmt.Sprintf("createVolSuccessTriggerOperatorErr %v", err))
	return
}

func (c *Cluster) dealCreateMetaPartition(nodeAddr string, resp *proto.CreateMetaPartitionResponse) {
	if resp.Status == proto.CmdFailed {
		log.LogError(fmt.Sprintf("action[dealCreateMetaPartition],nodeAddr %v create meta range failed,err %v", nodeAddr, resp.Result))
		return
	}

	var (
		metaNode *MetaNode
		ns       *NameSpace
		mp       *MetaPartition
		mr       *MetaReplica
		err      error
	)
	if metaNode, err = c.getMetaNode(nodeAddr); err != nil {
		goto errDeal
	}
	if ns, err = c.getNamespace(resp.NsName); err != nil {
		goto errDeal
	}

	if mp, err = ns.getMetaPartitionById(resp.GroupId); err != nil {
		goto errDeal
	}

	mr = NewMetaReplica(mp.Start, mp.End, metaNode)
	mr.status = MetaPartitionReadWrite
	mp.AddReplica(mr)
	mp.AddHostsByReplica(mr, c, resp.NsName)
	mp.Lock()
	mp.checkAndRemoveMissMetaReplica(mr.Addr)
	mp.Unlock()
	return
errDeal:
	log.LogError(fmt.Sprintf("createVolSuccessTriggerOperatorErr %v", err))
	return
}

func (c *Cluster) dealMetaNodeHeartbeat(nodeAddr string, resp *proto.MetaNodeHeartbeatResponse) {
	var (
		metaNode *MetaNode
		err      error
		logMsg   string
	)

	if metaNode, err = c.getMetaNode(nodeAddr); err != nil {
		goto errDeal
	}

	logMsg = fmt.Sprintf("action[dealMetaNodeHeartbeat],metaNode:%v ReportTime:%v  success", metaNode.Addr, time.Now().Unix())
	log.LogDebug(logMsg)
	metaNode.setNodeAlive()
	metaNode.metaRangeInfo = resp.MetaPartitionInfo
	c.UpdateMetaNode(metaNode)
	metaNode.metaRangeCount = len(metaNode.metaRangeInfo)
	metaNode.metaRangeInfo = nil

	return
errDeal:
	logMsg = fmt.Sprintf("nodeAddr %v hearbeat error :%v", nodeAddr, err.Error())
	log.LogError(logMsg)
	return
}

func (c *Cluster) dealDataNodeTaskResponse(nodeAddr string, task *proto.AdminTask) {
	if task == nil {
		return
	}

	dataNode, err := c.getDataNode(nodeAddr)
	if err != nil {
		return
	}
	if _, ok := dataNode.sender.taskMap[task.ID]; !ok {
		return
	}
	if err := UnmarshalTaskResponse(task); err != nil {
		return
	}
	switch task.OpCode {
	case OpCreateVol:
		response := task.Response.(*proto.CreateVolResponse)
		c.dealCreateVolResponse(task, response)
	case OpDeleteVol:
		response := task.Response.(*proto.DeleteVolResponse)
		c.dealDeleteVolResponse(task.OperatorAddr, response)
	case OpLoadVol:
		response := task.Response.(*proto.LoadVolResponse)
		c.dealLoadVolResponse(task.OperatorAddr, response)
	case OpDeleteFile:
		response := task.Response.(*proto.DeleteFileResponse)
		c.dealDeleteFileResponse(task.OperatorAddr, response)
	case OpDataNodeHeartbeat:
		response := task.Response.(*proto.DataNodeHeartBeatResponse)
		c.dealDataNodeHeartbeat(task.OperatorAddr, response)
	default:
		log.LogError(fmt.Sprintf("unknown operate code %v", task.OpCode))
	}

	return
}

func (c *Cluster) dealCreateVolResponse(t *proto.AdminTask, resp *proto.CreateVolResponse) {
	if resp.Status == proto.CmdSuccess {
		c.createVolSuccessTriggerOperator(t.OperatorAddr, resp)
	} else if resp.Status == proto.CmdFailed {
		c.createVolFailTriggerOperator(t, resp)
	}

	return
}

func (c *Cluster) createVolSuccessTriggerOperator(nodeAddr string, resp *proto.CreateVolResponse) {
	var (
		dataNode *DataNode
		vg       *VolGroup
		err      error
		vol      *Vol
	)

	if vg, err = c.getVolGroupByVolID(resp.VolId); err != nil {
		goto errDeal
	}

	if dataNode, err = c.getDataNode(nodeAddr); err != nil {
		goto errDeal
	}
	vol = NewVol(dataNode)
	vol.status = VolReadWrite
	vg.addMember(vol)

	vg.Lock()
	vg.checkAndRemoveMissVol(vol.addr)
	vg.Unlock()
	return
errDeal:
	log.LogError(fmt.Sprintf("createVolSuccessTriggerOperatorErr %v", err))
	return
}

func (c *Cluster) createVolFailTriggerOperator(t *proto.AdminTask, resp *proto.CreateVolResponse) (err error) {
	msg := fmt.Sprintf("action[createVolFailTriggerOperator],taskID:%v, vol:%v on :%v  "+
		"Fail And TrigerChangeOpAddr Fail:%v ", t.ID, resp.VolId, t.OperatorAddr, err)
	log.LogWarn(msg)

	return
}

func (c *Cluster) dealDeleteVolResponse(nodeAddr string, resp *proto.DeleteVolResponse) {
	var (
		vg  *VolGroup
		err error
	)
	if resp.Status == proto.CmdSuccess {
		if vg, err = c.getVolGroupByVolID(resp.VolId); err != nil {
			return
		}
		vg.Lock()
		vg.volOffLineInMem(nodeAddr)
		vg.Unlock()
	}

	return
}

func (c *Cluster) dealLoadVolResponse(nodeAddr string, resp *proto.LoadVolResponse) {
	var dataNode *DataNode
	vg, err := c.getVolGroupByVolID(resp.VolId)
	if err != nil || resp.Status == proto.CmdFailed || resp.VolSnapshot == nil {
		return
	}
	if dataNode, err = c.getDataNode(nodeAddr); err != nil {
		return
	}
	vg.LoadFile(dataNode, resp)

	return
}

func (c *Cluster) dealDeleteFileResponse(nodeAddr string, resp *proto.DeleteFileResponse) {
	var (
		vg  *VolGroup
		err error
	)
	if resp.Status == proto.CmdSuccess {
		if vg, err = c.getVolGroupByVolID(resp.VolId); err != nil {
			return
		}
		vg.DeleteFileOnNode(nodeAddr, resp.Name)
	}

	return
}

func (c *Cluster) dealDataNodeHeartbeat(nodeAddr string, resp *proto.DataNodeHeartBeatResponse) {

	var (
		dataNode *DataNode
		err      error
		logMsg   string
	)

	if dataNode, err = c.getDataNode(nodeAddr); err != nil {
		goto errDeal
	}

	logMsg = fmt.Sprintf("action[dealDataNodeHeartbeat],dataNode:%v ReportTime:%v  success", dataNode.HttpAddr, time.Now().Unix())
	log.LogDebug(logMsg)
	dataNode.setNodeAlive()
	dataNode.VolInfo = resp.VolInfo
	c.UpdateDataNode(dataNode)
	dataNode.VolInfoCount = len(dataNode.VolInfo)
	dataNode.VolInfo = nil

	return
errDeal:
	logMsg = fmt.Sprintf("nodeAddr %v hearbeat error :%v", nodeAddr, err.Error())
	log.LogError(logMsg)
	return
}

/*if node report volInfo,so range volInfo,then update volInfo*/
func (c *Cluster) UpdateDataNode(dataNode *DataNode) {
	for _, vr := range dataNode.VolInfo {
		if vr == nil {
			continue
		}
		if vol, err := c.getVolGroupByVolID(vr.VolID); err == nil {
			vol.UpdateVol(vr, dataNode)
		}
	}
}

func (c *Cluster) UpdateMetaNode(metaNode *MetaNode) {
	for _, mr := range metaNode.metaRangeInfo {
		if mr == nil {
			continue
		}
		if mp, err := c.getMetaPartitionByID(mr.GroupId); err == nil {
			mp.updateMetaPartition(mr, metaNode)
		}
	}
}
