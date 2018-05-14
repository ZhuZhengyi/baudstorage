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
		vg.checkReplicaNum()
		if vg.status == VolReadWrite {
			newReadWriteVolGroups++
		}
		volDiskErrorAddrs := vg.checkVolDiskError()
		if volDiskErrorAddrs != nil {
			for _, addr := range volDiskErrorAddrs {
				c.volOffline(addr, vg, CheckVolDiskErrorErr)
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
	ns.metaGroupLock.RLock()
	defer ns.metaGroupLock.RUnlock()
	var tasks []*proto.AdminTask
	for _, mg := range ns.MetaGroups {
		mg.checkStatus(true, int(ns.mrReplicaNum))
		mg.checkReplicas()
		tasks = append(tasks, mg.generateReplicaTask()...)
		tasks = append(tasks, mg.checkThreshold(ns.threshold, ns.mrSize))
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
	case OpCreateMetaGroup:
		response := task.Response.(*proto.CreateMetaRangeResponse)
		c.dealCreateMetaRange(task.OperatorAddr, response)
	case OpMetaNodeHeartbeat:
		response := task.Response.(*proto.MetaNodeHeartbeatResponse)
		c.dealMetaNodeHeartbeat(task.OperatorAddr, response)
	case OpDeleteMetaRange:
		response := task.Response.(*proto.DeleteMetaRangeResponse)
		c.dealDeleteMetaRange(task.OperatorAddr, response)
	case OpUpdateMetaRange:
		response := task.Response.(*proto.UpdateMetaRangeResponse)
		c.dealUpdateMetaRange(task.OperatorAddr, response)
	default:
		log.LogError(fmt.Sprintf("unknown operate code %v", task.OpCode))
	}

	return

errDeal:
	log.LogError(fmt.Sprintf("action[dealMetaNodeTaskResponse],nodeAddr %v,taskId %v,err %v",
		nodeAddr, task.ID, err.Error()))
	return
}

func (c *Cluster) dealUpdateMetaRange(nodeAddr string, resp *proto.UpdateMetaRangeResponse) {
	if resp.Status == proto.CmdFailed {
		log.LogError(fmt.Sprintf("action[dealUpdateMetaRange],nodeAddr %v update meta range failed,err %v", nodeAddr, resp.Result))
		return
	}
	mg, err := c.getMetaGroupByID(resp.GroupId)
	mg.End = resp.End
	mg.updateEnd()
	if err != nil {
		goto errDeal
	}
	if err = c.CreateMetaGroup(resp.NsName, mg.End, DefaultMaxMetaTabletRange); err != nil {
		goto errDeal
	}
	return
errDeal:
	log.LogError(fmt.Sprintf("createVolSuccessTriggerOperatorErr %v", err))
	return
}

func (c *Cluster) dealDeleteMetaRange(nodeAddr string, resp *proto.DeleteMetaRangeResponse) {
	if resp.Status == proto.CmdFailed {
		log.LogError(fmt.Sprintf("action[dealDeleteMetaRange],nodeAddr %v delete meta range failed,err %v", nodeAddr, resp.Result))
		return
	}
	var mr *MetaRange
	mg, err := c.getMetaGroupByID(resp.GroupId)
	if err != nil {
		goto errDeal
	}
	if mr, err = mg.getMetaRange(nodeAddr); err != nil {
		goto errDeal
	}
	mg.RemoveMember(mr)
	return

errDeal:
	log.LogError(fmt.Sprintf("createVolSuccessTriggerOperatorErr %v", err))
	return
}

func (c *Cluster) dealCreateMetaRange(nodeAddr string, resp *proto.CreateMetaRangeResponse) {
	if resp.Status == proto.CmdFailed {
		log.LogError(fmt.Sprintf("action[dealCreateMetaRange],nodeAddr %v create meta range failed,err %v", nodeAddr, resp.Result))
		return
	}

	var (
		metaNode *MetaNode
		ns       *NameSpace
		mg       *MetaGroup
		mr       *MetaRange
		err      error
	)
	if metaNode, err = c.getMetaNode(nodeAddr); err != nil {
		goto errDeal
	}
	if ns, err = c.getNamespace(resp.NsName); err != nil {
		goto errDeal
	}

	if mg, err = ns.getMetaGroupById(resp.GroupId); err != nil {
		goto errDeal
	}

	mr = NewMetaRange(mg.Start, mg.End, metaNode.id, metaNode.Addr)
	mr.status = MetaRangeReadWrite
	mg.AddMember(mr)
	mg.Lock()
	mg.checkAndRemoveMissMetaRange(mr.Addr)
	mg.Unlock()
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
	metaNode.metaRangeInfo = resp.MetaRangeInfo
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
		if mg, err := c.getMetaGroupByID(mr.GroupId); err == nil {
			mg.updateMetaGroup(mr, metaNode)
		}
	}
}
