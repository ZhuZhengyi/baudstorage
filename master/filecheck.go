package master

import (
	"fmt"
	"github.com/tiglabs/baudstorage/proto"
	"github.com/tiglabs/baudstorage/util/log"
)

/*check File: recover File,if File lack or timeOut report or crc bad*/
func (vg *VolGroup) checkFile(isRecoverVolFlag bool) (tasks []*proto.AdminTask) {
	tasks = make([]*proto.AdminTask, 0)
	vg.Lock()
	defer vg.Unlock()
	liveVols := vg.getLiveVols(DefaultVolTimeOutSec)
	if len(liveVols) == 0 {
		return
	}

	switch vg.volType {
	case ExtentVol:
		vg.checkExtentFile(liveVols, isRecoverVolFlag)
	case ChunkVol:
		vg.checkChunkFile(liveVols)
	}

	return
}

func (vg *VolGroup) checkChunkFile(liveVols []*Vol) (tasks []*proto.AdminTask) {
	for _, fc := range vg.FileInCoreMap {
		tasks = append(tasks, fc.generateFileCrcTask(vg.VolID, liveVols, ChunkVol)...)
	}
	return
}

func (vg *VolGroup) checkExtentFile(liveVols []*Vol, isRecoverVolFlag bool) (tasks []*proto.AdminTask) {
	for _, fc := range vg.FileInCoreMap {
		if fc.MarkDel == true {
			tasks = append(tasks, fc.generatorDeleteFileTask(vg.VolID)...)
			continue
		}
		if isRecoverVolFlag == true {
			tasks = append(tasks, fc.generatorLackFileTask(vg.VolID, liveVols)...)
		} else {
			if fc.isDelayCheck() {
				tasks = append(tasks, fc.generatorLackFileTask(vg.VolID, liveVols)...)
			}
			tasks = append(tasks, fc.generateFileCrcTask(vg.VolID, liveVols, ExtentVol)...)
		}
	}
	return
}

/*if File on a node is delete by other  ,so need recover it */
func (fc *FileInCore) generatorLackFileTask(volID uint64, liveVols []*Vol) (tasks []*proto.AdminTask) {
	tasks = make([]*proto.AdminTask, 0)
	for i := 0; i < len(liveVols); i++ {
		lackLoc := liveVols[i]
		if _, ok := fc.getFileMetaByVolAddr(lackLoc); ok {
			continue
		}
		msg := fc.generatorLackFileLog(volID, lackLoc, liveVols)
		if lackLoc.status == VolUnavailable {
			msg = msg + fmt.Sprintf(" ,But volLocationStatus :%v  So Can't do Replicat Task",
				lackLoc.status)
			log.LogWarn(msg)
			continue
		}

		if t := fc.generatorReplicateFileTask(volID, lackLoc, liveVols); t != nil {
			msg = msg + fmt.Sprintf(" from :%v ", t.OperatorAddr)
			tasks = append(tasks, t)
		} else {
			msg = msg + fmt.Sprintf(" no avail replicat to repair it")
		}
		log.LogWarn(msg)
	}

	return
}

func (fc *FileInCore) generatorLackFileLog(volID uint64, lackLoc *Vol, liveLocs []*Vol) (log string) {
	allNodeInfoAddrs := make([]string, 0)
	allLiveLocsAddrs := make([]string, len(liveLocs))
	for _, fm := range fc.Metas {
		allNodeInfoAddrs = append(allNodeInfoAddrs, fm.getLocationAddr())
	}
	for _, volLoc := range liveLocs {
		allLiveLocsAddrs = append(allLiveLocsAddrs, volLoc.addr)
	}

	log = fmt.Sprintf(GetLackFileNodeTaskErr+"check volID:%v  File:%v  "+
		"on Node:%v  currPtr[%p] AllLiveLocsPtr:%v  AllLiveLocsAddr:%v  "+
		"allNodeInfosPtr:%v  AllNodeInfoAddrs:%v  Not Exsit,So Must Repair it  ",
		volID, fc.Name, lackLoc.addr, lackLoc, liveLocs, allLiveLocsAddrs, fc.Metas,
		allNodeInfoAddrs)

	return
}

func (fc *FileInCore) generatorReplicateFileTask(volID uint64, badLoc *Vol, liveLocs []*Vol) (t *proto.AdminTask) {
	return proto.NewAdminTask(OpReplicateFile, badLoc.addr, nil)
}
