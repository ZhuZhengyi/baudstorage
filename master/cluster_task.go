package master

import (
	"fmt"
	"github.com/juju/errors"
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
			log.LogWarn(fmt.Sprintf("action[putTasks],nodeAddr:%v,taskID:%v,err:%v", node.Addr, t.ID, err.Error()))
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
			node.Sender.PutTask(t)
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
		if vg.Status == VolReadWrite {
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
	ns.volGroups.updateVolResponseCache(true, 0)
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
		newHosts   []string
		newPeers   []proto.Peer
		removePeer proto.Peer
	)
	log.LogDebugf("action[metaPartitionOffline],nsName[%v],nodeAddr[%v],partitionID[%v]", nsName, nodeAddr, partitionID)
	if ns, err = c.getNamespace(nsName); err != nil {
		goto errDeal
	}
	if mp, err = ns.getMetaPartition(partitionID); err != nil {
		goto errDeal
	}

	if !contains(mp.PersistenceHosts, nodeAddr) {
		return
	}

	if err = mp.canOffline(nodeAddr, int(ns.mpReplicaNum)); err != nil {
		goto errDeal
	}

	racks = mp.getRacks(nodeAddr)
	if newHosts, newPeers, err = c.getAvailMetaNodeHosts(racks[0], mp.PersistenceHosts, 1); err != nil {
		goto errDeal
	}
	for _, mr := range mp.Replicas {
		if mr.Addr == nodeAddr {
			removePeer = proto.Peer{ID: mr.nodeId, Addr: mr.Addr}
		} else {
			newPeers = append(newPeers, proto.Peer{ID: mr.nodeId, Addr: mr.Addr})
			newHosts = append(newHosts, mr.Addr)
		}
	}
	tasks = mp.generateCreateMetaPartitionTasks(newHosts, nsName)
	if t, err = mp.generateOfflineTask(nsName, removePeer, newPeers[0]); err != nil {
		goto errDeal
	}
	tasks = append(tasks, t)
	if err = mp.updateInfoToStore(newHosts, newPeers, nsName, c); err != nil {
		goto errDeal
	}
	mp.RemoveReplicaByAddr(nodeAddr)
	mp.checkAndRemoveMissMetaReplica(nodeAddr)
	c.putMetaNodeTasks(tasks)
	Warn(c.Name, fmt.Sprintf("meta partition[%v] offline addr[%v] success", partitionID, nodeAddr))
	return
errDeal:
	log.LogError(fmt.Sprintf("action[metaPartitionOffline],nsName: %v,partitionID: %v,err: %v",
		nsName, partitionID, errors.ErrorStack(err)))
	Warn(c.Name, fmt.Sprintf("meta partition[%v] offline addr[%v] failed,err:%v", partitionID, nodeAddr, err))
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
	checkFileTasks := v.checkFile(isRecover, c.Name)
	v.setVolToNormal()
	c.putDataNodeTasks(checkFileTasks)
}

func (c *Cluster) checkMetaGroups(ns *NameSpace) {
	ns.metaPartitionLock.RLock()
	defer ns.metaPartitionLock.RUnlock()
	var tasks []*proto.AdminTask
	for _, mp := range ns.MetaPartitions {
		mp.checkStatus(true, int(ns.mpReplicaNum))
		mp.checkReplicas(c, ns.Name, ns.mpReplicaNum)
		mp.checkReplicaMiss()
		tasks = append(tasks, mp.generateReplicaTask(ns.Name)...)
	}
	c.putMetaNodeTasks(tasks)

}

func (c *Cluster) dealMetaNodeTaskResponse(nodeAddr string, task *proto.AdminTask) (err error) {
	if task == nil {
		return
	}
	log.LogDebugf(fmt.Sprintf("action[dealMetaNodeTaskResponse] receive Task response:%v from %v", task.ID, nodeAddr))
	var (
		metaNode   *MetaNode
		taskStatus uint8
	)

	if metaNode, err = c.getMetaNode(nodeAddr); err != nil {
		goto errDeal
	}
	if _, ok := metaNode.Sender.TaskMap[task.ID]; !ok {
		err = taskNotFound(task.ID)
		goto errDeal
	}
	if err = UnmarshalTaskResponse(task); err != nil {
		goto errDeal
	}

	switch task.OpCode {
	case proto.OpCreateMetaPartition:
		response := task.Response.(*proto.CreateMetaPartitionResponse)
		taskStatus = response.Status
		err = c.dealCreateMetaPartitionResp(task.OperatorAddr, response)
	case proto.OpMetaNodeHeartbeat:
		response := task.Response.(*proto.MetaNodeHeartbeatResponse)
		taskStatus = response.Status
		err = c.dealMetaNodeHeartbeatResp(task.OperatorAddr, response)
	case proto.OpDeleteMetaPartition:
		response := task.Response.(*proto.DeleteMetaPartitionResponse)
		taskStatus = response.Status
		err = c.dealDeleteMetaPartitionResp(task.OperatorAddr, response)
	case proto.OpUpdateMetaPartition:
		response := task.Response.(*proto.UpdateMetaPartitionResponse)
		taskStatus = response.Status
		err = c.dealUpdateMetaPartitionResp(task.OperatorAddr, response)
	case proto.OpLoadMetaPartition:
		response := task.Response.(*proto.LoadMetaPartitionMetricResponse)
		taskStatus = response.Status
		err = c.dealLoadMetaPartitionResp(task.OperatorAddr, response)
	case proto.OpOfflineMetaPartition:
		response := task.Response.(*proto.MetaPartitionOfflineResponse)
		taskStatus = response.Status
		err = c.dealOfflineMetaPartitionResp(task.OperatorAddr, response)
	default:
		log.LogError(fmt.Sprintf("unknown operate code %v", task.OpCode))
	}

	if err != nil {
		log.LogError(fmt.Sprintf("process task[%v] failed", task.ToString()))
	} else {
		log.LogDebugf("task:%v status:%v", task.ID, task.Status)
		task.Status = int8(taskStatus)
	}

	if task.CheckTaskIsSuccess() {
		metaNode.Sender.DelTask(task)
	}

	return

errDeal:
	log.LogError(fmt.Sprintf("action[dealMetaNodeTaskResponse],nodeAddr %v,taskId %v,err %v",
		nodeAddr, task.ID, err.Error()))
	return
}

func (c *Cluster) dealOfflineMetaPartitionResp(nodeAddr string, resp *proto.MetaPartitionOfflineResponse) (err error) {
	if resp.Status == proto.TaskFail {
		msg := fmt.Sprintf("action[dealOfflineMetaPartitionResp],nodeAddr %v offline meta partition failed,err %v", nodeAddr, resp.Result)
		log.LogError(msg)
		Warn(c.Name, msg)
		return
	}
	return
}

func (c *Cluster) dealLoadMetaPartitionResp(nodeAddr string, resp *proto.LoadMetaPartitionMetricResponse) (err error) {
	return
}

func (c *Cluster) dealUpdateMetaPartitionResp(nodeAddr string, resp *proto.UpdateMetaPartitionResponse) (err error) {
	if resp.Status == proto.TaskFail {
		msg := fmt.Sprintf("action[dealUpdateMetaPartitionResp],nodeAddr %v update meta range failed,err %v", nodeAddr, resp.Result)
		log.LogError(msg)
		Warn(c.Name, msg)
		return
	}
	mp, err := c.getMetaPartitionByID(resp.PartitionID)
	if err != nil {
		goto errDeal
	}
	if err = c.CreateMetaPartition(resp.NsName, mp.End, DefaultMaxMetaPartitionInodeID); err != nil {
		goto errDeal
	}
	return
errDeal:
	log.LogError(fmt.Sprintf("dealUpdateMetaPartitionResp err %v", err))
	return
}

func (c *Cluster) dealDeleteMetaPartitionResp(nodeAddr string, resp *proto.DeleteMetaPartitionResponse) (err error) {
	if resp.Status == proto.TaskFail {
		msg := fmt.Sprintf("action[dealDeleteMetaPartitionResp],nodeAddr %v delete meta range failed,err %v", nodeAddr, resp.Result)
		log.LogError(msg)
		Warn(c.Name, msg)
		return
	}
	var mr *MetaReplica
	mp, err := c.getMetaPartitionByID(resp.PartitionID)
	if err != nil {
		goto errDeal
	}
	if mr, err = mp.getMetaReplica(nodeAddr); err != nil {
		goto errDeal
	}
	mp.RemoveReplica(mr)
	return

errDeal:
	log.LogError(fmt.Sprintf("dealDeleteMetaPartitionResp %v", err))
	return
}

func (c *Cluster) dealCreateMetaPartitionResp(nodeAddr string, resp *proto.CreateMetaPartitionResponse) (err error) {
	if resp.Status == proto.TaskFail {
		msg := fmt.Sprintf("action[dealCreateMetaPartitionResp],nodeAddr %v create meta range failed,err %v", nodeAddr, resp.Result)
		log.LogError(msg)
		Warn(c.Name, msg)
		return
	}

	var (
		metaNode *MetaNode
		ns       *NameSpace
		mp       *MetaPartition
		mr       *MetaReplica
	)
	if metaNode, err = c.getMetaNode(nodeAddr); err != nil {
		goto errDeal
	}
	if ns, err = c.getNamespace(resp.NsName); err != nil {
		goto errDeal
	}

	if mp, err = ns.getMetaPartition(resp.PartitionID); err != nil {
		goto errDeal
	}

	mr = NewMetaReplica(mp.Start, mp.End, metaNode)
	mr.Status = MetaPartitionReadWrite
	mp.AddReplica(mr)
	mp.checkAndRemoveMissMetaReplica(mr.Addr)
	return
errDeal:
	log.LogErrorf(fmt.Sprintf("action[dealCreateMetaPartitionResp] %v", errors.ErrorStack(err)))
	return
}

func (c *Cluster) dealMetaNodeHeartbeatResp(nodeAddr string, resp *proto.MetaNodeHeartbeatResponse) (err error) {
	var (
		metaNode *MetaNode
		logMsg   string
	)

	if resp.Status == proto.TaskFail {
		msg := fmt.Sprintf("action[dealMetaNodeHeartbeatResp],nodeAddr %v heartbeat failed,err %v", nodeAddr, resp.Result)
		log.LogError(msg)
		Warn(c.Name, msg)
		return
	}

	if metaNode, err = c.getMetaNode(nodeAddr); err != nil {
		goto errDeal
	}

	logMsg = fmt.Sprintf("action[dealMetaNodeHeartbeatResp],metaNode:%v ReportTime:%v  success", metaNode.Addr, time.Now().Unix())
	log.LogDebug(logMsg)
	metaNode.updateMetric(resp)
	c.UpdateMetaNode(metaNode, metaNode.isArriveThreshold())
	metaNode.metaRangeInfos = nil

	return
errDeal:
	logMsg = fmt.Sprintf("nodeAddr %v heartbeat error :%v", nodeAddr, errors.ErrorStack(err))
	log.LogError(logMsg)
	return
}

func (c *Cluster) dealDataNodeTaskResponse(nodeAddr string, task *proto.AdminTask) {
	if task == nil {
		log.LogDebugf("action[dealDataNodeTaskResponse] receive addr[%v] task response,but task is nil", nodeAddr)
		return
	}
	log.LogDebugf("action[dealDataNodeTaskResponse] receive addr[%v] task response:%v", nodeAddr, task.ToString())
	var (
		err        error
		dataNode   *DataNode
		taskStatus uint8
	)
	dataNode, err = c.getDataNode(nodeAddr)
	if err != nil {
		goto errDeal
	}
	if _, ok := dataNode.sender.TaskMap[task.ID]; !ok {
		err = taskNotFound(task.ID)
		goto errDeal
	}
	if err := UnmarshalTaskResponse(task); err != nil {
		goto errDeal
	}

	switch task.OpCode {
	case proto.OpCreateDataPartion:
		response := task.Response.(*proto.CreateVolResponse)
		taskStatus = response.Status
		err = c.dealCreateVolResponse(task, response)
	case proto.OpDeleteDataPartion:
		response := task.Response.(*proto.DeleteVolResponse)
		taskStatus = response.Status
		err = c.dealDeleteVolResponse(task.OperatorAddr, response)
	case proto.OpLoadDataPartion:
		response := task.Response.(*proto.LoadVolResponse)
		taskStatus = response.Status
		err = c.dealLoadVolResponse(task.OperatorAddr, response)
	case proto.OpDeleteFile:
		response := task.Response.(*proto.DeleteFileResponse)
		taskStatus = response.Status
		err = c.dealDeleteFileResponse(task.OperatorAddr, response)
	case proto.OpDataNodeHeartbeat:
		response := task.Response.(*proto.DataNodeHeartBeatResponse)
		taskStatus = response.Status
		err = c.dealDataNodeHeartbeatResp(task.OperatorAddr, response)
	default:
		err = fmt.Errorf(fmt.Sprintf("unknown operate code %v", task.OpCode))
		goto errDeal
	}

	if err != nil {
		goto errDeal
	}
	task.SetStatus(int8(taskStatus))
	return

errDeal:
	log.LogErrorf("process task[%v] failed,err:%v", task.ToString(), err)
	return
}

func (c *Cluster) dealCreateVolResponse(t *proto.AdminTask, resp *proto.CreateVolResponse) (err error) {
	if resp.Status == proto.TaskSuccess {
		c.createVolSuccessTriggerOperator(t.OperatorAddr, resp)
	} else if resp.Status == proto.TaskFail {
		c.createVolFailTriggerOperator(t, resp)
	}

	return
}

func (c *Cluster) createVolSuccessTriggerOperator(nodeAddr string, resp *proto.CreateVolResponse) (err error) {
	var (
		dataNode *DataNode
		vg       *VolGroup
		vol      *Vol
	)

	if vg, err = c.getVolGroupByVolID(resp.VolId); err != nil {
		goto errDeal
	}

	if dataNode, err = c.getDataNode(nodeAddr); err != nil {
		goto errDeal
	}
	vol = NewVol(dataNode)
	vol.Status = VolReadWrite
	vg.addMember(vol)

	vg.Lock()
	vg.checkAndRemoveMissVol(vol.Addr)
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

func (c *Cluster) dealDeleteVolResponse(nodeAddr string, resp *proto.DeleteVolResponse) (err error) {
	var (
		vg *VolGroup
	)
	if resp.Status == proto.TaskSuccess {
		if vg, err = c.getVolGroupByVolID(resp.VolId); err != nil {
			return
		}
		vg.Lock()
		vg.volOffLineInMem(nodeAddr)
		vg.Unlock()
	} else {
		Warn(c.Name, fmt.Sprintf("delete vol[%v] failed", nodeAddr))
	}

	return
}

func (c *Cluster) dealLoadVolResponse(nodeAddr string, resp *proto.LoadVolResponse) (err error) {
	var dataNode *DataNode
	vg, err := c.getVolGroupByVolID(resp.VolId)
	if err != nil || resp.Status == proto.TaskFail || resp.VolSnapshot == nil {
		return
	}
	if dataNode, err = c.getDataNode(nodeAddr); err != nil {
		return
	}
	vg.LoadFile(dataNode, resp)

	return
}

func (c *Cluster) dealDeleteFileResponse(nodeAddr string, resp *proto.DeleteFileResponse) (err error) {
	var (
		vg *VolGroup
	)
	if resp.Status == proto.TaskSuccess {
		if vg, err = c.getVolGroupByVolID(resp.VolId); err != nil {
			return
		}
		vg.DeleteFileOnNode(nodeAddr, resp.Name)
	}

	return
}

func (c *Cluster) dealDataNodeHeartbeatResp(nodeAddr string, resp *proto.DataNodeHeartBeatResponse) (err error) {

	var (
		dataNode *DataNode
		logMsg   string
	)

	if resp.Status != proto.TaskSuccess {
		Warn(c.Name, fmt.Sprintf("dataNode[%v] heartbeat task failed", nodeAddr))
		return
	}

	if dataNode, err = c.getDataNode(nodeAddr); err != nil {
		goto errDeal
	}

	dataNode.UpdateNodeMetric(resp)
	dataNode.setNodeAlive()
	c.UpdateDataNode(dataNode)
	dataNode.VolInfo = nil
	logMsg = fmt.Sprintf("action[dealDataNodeHeartbeatResp],dataNode:%v ReportTime:%v  success", dataNode.Addr, time.Now().Unix())
	log.LogDebug(logMsg)

	return
errDeal:
	logMsg = fmt.Sprintf("nodeAddr %v heartbeat error :%v", nodeAddr, err.Error())
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

func (c *Cluster) UpdateMetaNode(metaNode *MetaNode, threshold bool) {
	tasks := make([]*proto.AdminTask, 0)
	for _, mr := range metaNode.metaRangeInfos {
		if mr == nil {
			continue
		}
		mp, err := c.getMetaPartitionByID(mr.PartitionID)
		if err != nil {
			log.LogError(fmt.Sprintf("action[UpdateMetaNode],err:%v", err))
			err = nil
			continue
		}
		mp.updateMetaPartition(mr, metaNode)
		if threshold {
			end := mr.MaxInodeID + mr.End + DefaultMetaPartitionInodeIDStep
			mp.updateEnd()
			t := mp.generateUpdateMetaReplicaTask(mp.PartitionID, end)
			tasks = append(tasks, t)
		}
	}
	c.putMetaNodeTasks(tasks)
}
