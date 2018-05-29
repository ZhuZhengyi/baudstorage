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

	if err = mp.canOffline(nodeAddr, int(ns.mpReplicaNum)); err != nil {
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
	mp.Peers = peers
	tasks = mp.generateCreateMetaPartitionTasks(newHosts, nsName)
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
		mp.checkReplicas(c, ns.Name, ns.mpReplicaNum)
		tasks = append(tasks, mp.generateReplicaTask(ns.Name)...)
	}
	c.putMetaNodeTasks(tasks)

}

func (c *Cluster) dealMetaNodeTaskResponse(nodeAddr string, task *proto.AdminTask) (err error) {
	if task == nil {
		return
	}
	log.LogDebugf(fmt.Sprintf("receive task response:%v from %v", task.ID, nodeAddr))
	var (
		metaNode   *MetaNode
		taskStatus uint8
	)

	if metaNode, err = c.getMetaNode(nodeAddr); err != nil {
		goto errDeal
	}
	//if _, ok := metaNode.Sender.TaskMap[task.ID]; !ok {
	//	err = taskNotFound(task.ID)
	//	goto errDeal
	//}
	if err = UnmarshalTaskResponse(task); err != nil {
		goto errDeal
	}

	switch task.OpCode {
	case proto.OpCreateMetaPartition:
		response := task.Response.(*proto.CreateMetaPartitionResponse)
		taskStatus = response.Status
		err = c.dealCreateMetaPartition(task.OperatorAddr, response)
	case proto.OpMetaNodeHeartbeat:
		response := task.Response.(*proto.MetaNodeHeartbeatResponse)
		taskStatus = response.Status
		err = c.dealMetaNodeHeartbeat(task.OperatorAddr, response)
	case proto.OpDeleteMetaPartition:
		response := task.Response.(*proto.DeleteMetaPartitionResponse)
		taskStatus = response.Status
		err = c.dealDeleteMetaPartition(task.OperatorAddr, response)
	case proto.OpUpdateMetaPartition:
		response := task.Response.(*proto.UpdateMetaPartitionResponse)
		taskStatus = response.Status
		err = c.dealUpdateMetaPartition(task.OperatorAddr, response)
	case proto.OpLoadMetaPartition:
		response := task.Response.(*proto.LoadMetaPartitionMetricResponse)
		taskStatus = response.Status
		err = c.dealLoadMetaPartition(task.OperatorAddr, response)
	case proto.OpOfflineMetaPartition:
		response := task.Response.(*proto.MetaPartitionOfflineResponse)
		taskStatus = response.Status
		err = c.dealOfflineMetaPartition(task.OperatorAddr, response)
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

func (c *Cluster) dealOfflineMetaPartition(nodeAddr string, resp *proto.MetaPartitionOfflineResponse) (err error) {
	if resp.Status == proto.TaskFail {
		log.LogError(fmt.Sprintf("action[dealOfflineMetaPartition],nodeAddr %v offline meta partition failed,err %v", nodeAddr, resp.Result))
		return
	}
	mp, err := c.getMetaPartitionByID(resp.PartitionID)
	if err != nil {
		goto errDeal
	}
	if err = mp.removePersistenceHosts(nodeAddr, c, resp.NsName); err != nil {
		goto errDeal
	}
	mp.checkAndRemoveMissMetaReplica(nodeAddr)
	return
errDeal:
	log.LogError(fmt.Sprintf("dealOfflineMetaPartition err: %v", err.Error()))
	return
}

func (c *Cluster) dealLoadMetaPartition(nodeAddr string, resp *proto.LoadMetaPartitionMetricResponse) (err error) {
	return
}

func (c *Cluster) dealUpdateMetaPartition(nodeAddr string, resp *proto.UpdateMetaPartitionResponse) (err error) {
	if resp.Status == proto.TaskFail {
		log.LogError(fmt.Sprintf("action[dealUpdateMetaPartition],nodeAddr %v update meta range failed,err %v", nodeAddr, resp.Result))
		return
	}
	mp, err := c.getMetaPartitionByID(resp.PartitionID)
	if err != nil {
		goto errDeal
	}
	if err = c.CreateMetaPartition(resp.NsName, mp.End, DefaultMaxMetaPartitionRange); err != nil {
		goto errDeal
	}
	return
errDeal:
	log.LogError(fmt.Sprintf("dealUpdateMetaPartition err %v", err))
	return
}

func (c *Cluster) dealDeleteMetaPartition(nodeAddr string, resp *proto.DeleteMetaPartitionResponse) (err error) {
	if resp.Status == proto.TaskFail {
		log.LogError(fmt.Sprintf("action[dealDeleteMetaPartition],nodeAddr %v delete meta range failed,err %v", nodeAddr, resp.Result))
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
	log.LogError(fmt.Sprintf("dealDeleteMetaPartition %v", err))
	return
}

func (c *Cluster) dealCreateMetaPartition(nodeAddr string, resp *proto.CreateMetaPartitionResponse) (err error) {
	if resp.Status == proto.TaskFail {
		log.LogError(fmt.Sprintf("action[dealCreateMetaPartition],nodeAddr %v create meta range failed,err %v", nodeAddr, resp.Result))
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

	if mp, err = ns.getMetaPartitionById(resp.PartitionID); err != nil {
		goto errDeal
	}

	mr = NewMetaReplica(mp.Start, mp.End, metaNode)
	mr.Status = MetaPartitionReadWrite
	mp.AddReplica(mr)
	mp.checkAndRemoveMissMetaReplica(mr.Addr)
	return
errDeal:
	log.LogError(fmt.Sprintf("dealCreateMetaPartition %v", err))
	return
}

func (c *Cluster) dealMetaNodeHeartbeat(nodeAddr string, resp *proto.MetaNodeHeartbeatResponse) (err error) {
	var (
		metaNode *MetaNode
		logMsg   string
	)

	if resp.Status != proto.TaskSuccess {
		return
	}

	if metaNode, err = c.getMetaNode(nodeAddr); err != nil {
		goto errDeal
	}

	logMsg = fmt.Sprintf("action[dealMetaNodeHeartbeat],metaNode:%v ReportTime:%v  success", metaNode.Addr, time.Now().Unix())
	log.LogDebug(logMsg)
	metaNode.updateMetric(resp)
	c.UpdateMetaNode(metaNode, metaNode.isArriveThreshold())
	metaNode.metaRangeInfos = nil

	return
errDeal:
	logMsg = fmt.Sprintf("nodeAddr %v heartbeat error :%v", nodeAddr, err.Error())
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
	case proto.OpCreateVol:
		response := task.Response.(*proto.CreateVolResponse)
		taskStatus = response.Status
		err = c.dealCreateVolResponse(task, response)
	case proto.OpDeleteVol:
		response := task.Response.(*proto.DeleteVolResponse)
		taskStatus = response.Status
		err = c.dealDeleteVolResponse(task.OperatorAddr, response)
	case proto.OpLoadVol:
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
		err = c.dealDataNodeHeartbeat(task.OperatorAddr, response)
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

func (c *Cluster) dealDataNodeHeartbeat(nodeAddr string, resp *proto.DataNodeHeartBeatResponse) (err error) {

	var (
		dataNode *DataNode
		logMsg   string
	)

	if resp.Status != proto.TaskSuccess {
		return
	}

	if dataNode, err = c.getDataNode(nodeAddr); err != nil {
		goto errDeal
	}

	dataNode.UpdateNodeMetric(resp)
	dataNode.setNodeAlive()
	c.UpdateDataNode(dataNode)
	dataNode.VolInfo = nil
	logMsg = fmt.Sprintf("action[dealDataNodeHeartbeat],dataNode:%v ReportTime:%v  success", dataNode.HttpAddr, time.Now().Unix())
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
			end := mr.MaxInodeID + mr.End + DefaultMinMetaPartitionRange
			mp.updateEnd()
			t := mp.generateUpdateMetaReplicaTask(mp.PartitionID, end)
			tasks = append(tasks, t)
		}
	}
	c.putMetaNodeTasks(tasks)
}
