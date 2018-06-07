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

func (c *Cluster) checkDataPartitions(ns *NameSpace) {
	ns.dataPartitions.RLock()
	newReadWriteDataPartitions := 0
	for _, dp := range ns.dataPartitions.dataPartitionMap {
		dp.checkReplicaStatus(c.cfg.DataPartitionTimeOutSec)
		dp.checkStatus(true, c.cfg.DataPartitionTimeOutSec)
		dp.checkMiss(c.Name, c.cfg.DataPartitionMissSec, c.cfg.DataPartitionWarnInterval)
		dp.checkReplicaNum(c, ns.Name)
		if dp.Status == DataPartitionReadWrite {
			newReadWriteDataPartitions++
		}
		diskErrorAddrs := dp.checkDiskError()
		if diskErrorAddrs != nil {
			for _, addr := range diskErrorAddrs {
				c.dataPartitionOffline(addr, ns.Name, dp, CheckDataPartitionDiskErrorErr)
			}
		}
		tasks := dp.checkReplicationTask()
		c.putDataNodeTasks(tasks)
	}
	ns.dataPartitions.readWriteDataPartitions = newReadWriteDataPartitions
	ns.dataPartitions.RUnlock()
	ns.dataPartitions.updateDataPartitionResponseCache(true, 0)
	msg := fmt.Sprintf("action[checkDataPartitions],can readWrite dataPartitions:%v  ", ns.dataPartitions.readWriteDataPartitions)
	log.LogInfo(msg)
}

func (c *Cluster) backendLoadDataPartition(ns *NameSpace) {
	needCheckDataPartitions := ns.dataPartitions.getNeedCheckDataPartitions(c.cfg.everyLoadDataPartitionCount, c.cfg.LoadDataPartitionFrequencyTime)
	if len(needCheckDataPartitions) == 0 {
		return
	}
	c.waitLoadDataPartitionResponse(needCheckDataPartitions)
	msg := fmt.Sprintf("action[backendLoadDataPartition] checkstart:%v everyCheckCount:%v",
		needCheckDataPartitions[0].PartitionID, c.cfg.everyLoadDataPartitionCount)
	log.LogInfo(msg)
}

func (c *Cluster) waitLoadDataPartitionResponse(partitions []*DataPartition) {

	defer func() {
		if err := recover(); err != nil {
			const size = RuntimeStackBufSize
			buf := make([]byte, size)
			buf = buf[:runtime.Stack(buf, false)]
			log.LogError(fmt.Sprintf("waitLoadDataPartitionResponse panic %v: %s\n", err, buf))
		}
	}()
	var wg sync.WaitGroup
	for _, v := range partitions {
		wg.Add(1)
		go func(v *DataPartition) {
			c.processLoadDataPartition(v, false)
			wg.Done()
		}(v)
	}
	wg.Wait()
}

func (c *Cluster) processReleaseDataPartitionAfterLoad(ns *NameSpace) {
	needReleaseDataPartitions := ns.dataPartitions.getNeedReleaseDataPartitions(c.cfg.everyReleaseDataPartitionCount, c.cfg.releaseDataPartitionAfterLoadSeconds)
	if len(needReleaseDataPartitions) == 0 {
		return
	}
	ns.dataPartitions.releaseDataPartitions(needReleaseDataPartitions)
	msg := fmt.Sprintf("action[processReleaseDataPartitionAfterLoad]  release data partition start:%v everyReleaseDataPartitionCount:%v",
		needReleaseDataPartitions[0].PartitionID, c.cfg.everyReleaseDataPartitionCount)
	log.LogInfo(msg)
}

func (c *Cluster) loadDataPartitionAndCheckResponse(v *DataPartition, isRecover bool) {
	go func() {
		c.processLoadDataPartition(v, isRecover)
	}()
}

func (c *Cluster) metaPartitionOffline(nsName, nodeAddr string, partitionID uint64) (err error) {
	var (
		ns         *NameSpace
		mp         *MetaPartition
		t          *proto.AdminTask
		tasks      []*proto.AdminTask
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

	if newHosts, newPeers, err = c.getAvailMetaNodeHosts(mp.PersistenceHosts, 1); err != nil {
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

func (c *Cluster) processLoadDataPartition(dp *DataPartition, isRecover bool) {
	log.LogInfo(fmt.Sprintf("action[processLoadDataPartition],partitionID:%v,isRecover:%v", dp.PartitionID, isRecover))
	loadTasks := dp.generateLoadTasks()
	c.putDataNodeTasks(loadTasks)
	for i := 0; i < LoadDataPartitionWaitTime; i++ {
		if dp.checkLoadResponse(c.cfg.DataPartitionTimeOutSec) {
			log.LogInfo(fmt.Sprintf("action[%v] triger all replication,partitionID:%v ", "loadDataPartitionAndCheckResponse", dp.PartitionID))
			break
		}
		time.Sleep(time.Second)
	}
	// response is time out
	if dp.checkLoadResponse(c.cfg.DataPartitionTimeOutSec) == false {
		return
	}
	dp.getFileCount()
	dp.checkFile(isRecover, c.Name)
	dp.setToNormal()
}

func (c *Cluster) checkMetaPartitions(ns *NameSpace) {
	ns.metaPartitionLock.RLock()
	defer ns.metaPartitionLock.RUnlock()
	var tasks []*proto.AdminTask
	for _, mp := range ns.MetaPartitions {
		mp.checkStatus(true, int(ns.mpReplicaNum))
		mp.checkReplicas(c, ns.Name, ns.mpReplicaNum)
		mp.checkReplicaMiss(c.Name, DefaultMetaPartitionTimeOutSec, DefaultMetaPartitionWarnInterval)
		tasks = append(tasks, mp.generateReplicaTask(ns.Name)...)
	}
	c.putMetaNodeTasks(tasks)

}

func (c *Cluster) dealMetaNodeTaskResponse(nodeAddr string, task *proto.AdminTask) (err error) {
	if task == nil {
		return
	}
	log.LogDebugf(fmt.Sprintf("action[dealMetaNodeTaskResponse] receive Task response:%v from %v", task.ToString(), nodeAddr))
	var (
		metaNode   *MetaNode
		taskStatus uint8
	)

	if metaNode, err = c.getMetaNode(nodeAddr); err != nil {
		goto errDeal
	}
	if isExist := metaNode.Sender.IsExist(task); !isExist {
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
	if isExist := dataNode.sender.IsExist(task); !isExist {
		err = taskNotFound(task.ID)
		goto errDeal
	}

	if err := UnmarshalTaskResponse(task); err != nil {
		goto errDeal
	}

	switch task.OpCode {
	case proto.OpCreateDataPartition:
		response := task.Response.(*proto.CreateDataPartitionResponse)
		taskStatus = response.Status
		err = c.dealCreateDataPartitionResponse(task, response)
	case proto.OpDeleteDataPartition:
		response := task.Response.(*proto.DeleteDataPartitionResponse)
		taskStatus = response.Status
		err = c.dealDeleteDataPartitionResponse(task.OperatorAddr, response)
	case proto.OpLoadDataPartition:
		response := task.Response.(*proto.LoadDataPartitionResponse)
		taskStatus = response.Status
		err = c.dealLoadDataPartitionResponse(task.OperatorAddr, response)
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

func (c *Cluster) dealCreateDataPartitionResponse(t *proto.AdminTask, resp *proto.CreateDataPartitionResponse) (err error) {
	if resp.Status == proto.TaskSuccess {
		c.createDataPartitionSuccessTriggerOperator(t.OperatorAddr, resp)
	} else if resp.Status == proto.TaskFail {
		c.createDataPartitionFailTriggerOperator(t, resp)
	}

	return
}

func (c *Cluster) createDataPartitionSuccessTriggerOperator(nodeAddr string, resp *proto.CreateDataPartitionResponse) (err error) {
	var (
		dataNode *DataNode
		dp       *DataPartition
		replica  *DataReplica
	)

	if dp, err = c.getDataPartitionByID(resp.PartitionId); err != nil {
		goto errDeal
	}

	if dataNode, err = c.getDataNode(nodeAddr); err != nil {
		goto errDeal
	}
	replica = NewDataReplica(dataNode)
	replica.Status = DataPartitionReadWrite
	dp.addMember(replica)

	dp.Lock()
	dp.checkAndRemoveMissReplica(replica.Addr)
	dp.Unlock()
	return
errDeal:
	log.LogError(fmt.Sprintf("createDataPartitionSuccessTriggerOperatorErr %v", err))
	return
}

func (c *Cluster) createDataPartitionFailTriggerOperator(t *proto.AdminTask, resp *proto.CreateDataPartitionResponse) (err error) {
	msg := fmt.Sprintf("action[createDataPartitionFailTriggerOperator],taskID:%v, partitionID:%v on :%v  "+
		"Fail And TrigerChangeOpAddr Fail:%v ", t.ID, resp.PartitionId, t.OperatorAddr, err)
	log.LogWarn(msg)
	return
}

func (c *Cluster) dealDeleteDataPartitionResponse(nodeAddr string, resp *proto.DeleteDataPartitionResponse) (err error) {
	var (
		dp *DataPartition
	)
	if resp.Status == proto.TaskSuccess {
		if dp, err = c.getDataPartitionByID(resp.PartitionId); err != nil {
			return
		}
		dp.Lock()
		dp.offLineInMem(nodeAddr)
		dp.Unlock()
	} else {
		Warn(c.Name, fmt.Sprintf("delete data partition[%v] failed", nodeAddr))
	}

	return
}

func (c *Cluster) dealLoadDataPartitionResponse(nodeAddr string, resp *proto.LoadDataPartitionResponse) (err error) {
	var dataNode *DataNode
	dp, err := c.getDataPartitionByID(resp.PartitionId)
	if err != nil || resp.Status == proto.TaskFail || resp.PartitionSnapshot == nil {
		return
	}
	if dataNode, err = c.getDataNode(nodeAddr); err != nil {
		return
	}
	dp.LoadFile(dataNode, resp)

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
	c.t.putDataNode(dataNode)
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

/*if node report data partition infos,so range data partition infos,then update data partition info*/
func (c *Cluster) UpdateDataNode(dataNode *DataNode) {
	for _, vr := range dataNode.VolInfo {
		if vr == nil {
			continue
		}
		if vol, err := c.getDataPartitionByID(vr.PartitionID); err == nil {
			vol.UpdateDataPartitionMetric(vr, dataNode)
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
