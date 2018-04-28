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

func (c *Cluster) dealTaskResponsePack(nodeAddr string, taskResps []*proto.AdminTask) {
	if taskResps == nil || len(taskResps) == 0 {
		return
	}
	for _, tr := range taskResps {
		if tr == nil {
			continue
		}
		log.LogDebug(fmt.Sprintf("action[%v],task:%v,nodeAddr:%v,response:%v", "dealTaskResponsePack", tr.ID, nodeAddr, nil))
		dataNode, err := c.getDataNode(nodeAddr)
		if err != nil {
			continue
		}
		if t, ok := dataNode.sender.taskMap[tr.ID]; ok {
			c.dealTaskResponse(tr, t)
		}
	}

	return
}

func (c *Cluster) dealTaskResponse(tr *proto.AdminTask, t *proto.AdminTask) {
	log.LogDebug(fmt.Sprintf("action[%v],task:%v,response:%v", "dealTaskResponse", tr.ID, nil))
	return
}
