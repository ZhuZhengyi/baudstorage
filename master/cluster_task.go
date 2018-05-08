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

func (c *Cluster) checkVolGroups() {
	c.volGroups.RLock()
	newReadWriteVolGroups := 0
	for _, vol := range c.volGroups.volGroupMap {
		vol.checkLocationStatus(c.cfg.VolTimeOutSec)
		vol.checkStatus(true, c.cfg.VolTimeOutSec)
		vol.checkVolGroupMiss(c.cfg.VolMissSec, c.cfg.VolWarnInterval)
		vol.checkReplicaNum()
		if vol.status == VolReadWrite {
			newReadWriteVolGroups++
		}
		vol.checkVolDiskError()
		volTasks := vol.checkVolReplicationTask()
		c.putDataNodeTasks(volTasks)
	}
	c.volGroups.readWriteVolGroups = newReadWriteVolGroups
	c.volGroups.RUnlock()
	c.volGroups.updateVolResponseCache(NeedUpdateVolResponse, 0)
	msg := fmt.Sprintf("action[CheckVolInfo],can readwrite vol:%v  ", c.volGroups.readWriteVolGroups)
	log.LogInfo(msg)
}

func (c *Cluster) backendLoadVolGroup(everyLoadVolCount int, loadVolFrequencyTime int64) {
	needCheckVols := c.volGroups.getNeedCheckVolGroups(everyLoadVolCount, loadVolFrequencyTime)
	if len(needCheckVols) == 0 {
		return
	}
	c.waitLoadVolResponse(needCheckVols)
	msg := fmt.Sprintf("action[BackendLoadVol] checkstart:%v everyCheckCount:%v",
		needCheckVols[0].VolID, everyLoadVolCount)
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

func (c *Cluster) processReleaseVolAfterLoadVolGroup() {
	needReleaseVols := c.volGroups.getNeedReleaseVolGroups(c.cfg.everyReleaseVolCount, c.cfg.releaseVolAfterLoadVolSeconds)
	if len(needReleaseVols) == 0 {
		return
	}
	c.volGroups.releaseVolGroups(needReleaseVols)
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

func (c *Cluster) dealMetaNodeTaskResponse(nodeAddr string, task *proto.AdminTask) {
	if task == nil {
		return
	}

	metaNode, err := c.getMetaNode(nodeAddr)
	if err != nil {
		return
	}
	if _, ok := metaNode.sender.taskMap[task.ID]; !ok {
		return
	}
	if err := UnmarshalTaskResponse(task); err != nil {
		return
	}

	switch task.OpCode {
	case OpCreateMetaGroup:
		response := task.Response.(*proto.CreateMetaRangeResponse)
		c.dealCreateMetaRange(task.OperatorAddr, response)
	default:
		log.LogError(fmt.Sprintf("unknown operate code %v", task.OpCode))
	}

	return
}

func (c *Cluster) dealCreateMetaRange(nodeAddr string, resp *proto.CreateMetaRangeResponse) {
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

	if vg, err = c.getVolByVolID(resp.VolId); err != nil {
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
		if vg, err = c.getVolByVolID(resp.VolId); err != nil {
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
	vg, err := c.getVolByVolID(resp.VolId)
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
		if vg, err = c.getVolByVolID(resp.VolId); err != nil {
			return
		}
		vg.DeleteFileOnNode(nodeAddr, resp.Name)
	}

	return
}
