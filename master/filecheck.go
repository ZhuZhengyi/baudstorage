package master

import (
	"fmt"
	"github.com/tiglabs/baudstorage/proto"
	"github.com/tiglabs/baudstorage/util/log"
)

/*check File: recover File,if File lack or timeOut report or crc bad*/
func (partition *DataPartition) checkFile(isRecoverVolFlag bool, clusterID string) (tasks []*proto.AdminTask) {
	tasks = make([]*proto.AdminTask, 0)
	partition.Lock()
	defer partition.Unlock()
	liveVols := partition.getLiveVols(DefaultVolTimeOutSec)
	if len(liveVols) == 0 {
		return
	}

	switch partition.PartitionType {
	case ExtentVol:
		partition.checkExtentFile(liveVols, isRecoverVolFlag, clusterID)
	case ChunkVol:
		partition.checkChunkFile(liveVols, clusterID)
	}

	return
}

func (partition *DataPartition) checkChunkFile(liveVols []*DataReplica, clusterID string) (tasks []*proto.AdminTask) {
	for _, fc := range partition.FileInCoreMap {
		tasks = append(tasks, fc.generateFileCrcTask(partition.PartitionID, liveVols, ChunkVol, clusterID)...)
	}
	return
}

func (partition *DataPartition) checkExtentFile(liveVols []*DataReplica, isRecoverVolFlag bool, clusterID string) (tasks []*proto.AdminTask) {
	for _, fc := range partition.FileInCoreMap {
		if fc.MarkDel == true {
			tasks = append(tasks, fc.generatorDeleteFileTask(partition.PartitionID)...)
			continue
		}
		if isRecoverVolFlag == true {
			tasks = append(tasks, fc.generatorLackFileTask(partition.PartitionID, liveVols)...)
		} else {
			if fc.isDelayCheck() {
				tasks = append(tasks, fc.generatorLackFileTask(partition.PartitionID, liveVols)...)
			}
			tasks = append(tasks, fc.generateFileCrcTask(partition.PartitionID, liveVols, ExtentVol, clusterID)...)
		}
	}
	return
}

/*if File on a node is delete by other  ,so need recover it */
func (fc *FileInCore) generatorLackFileTask(volID uint64, liveVols []*DataReplica) (tasks []*proto.AdminTask) {
	tasks = make([]*proto.AdminTask, 0)
	for i := 0; i < len(liveVols); i++ {
		lackLoc := liveVols[i]
		if _, ok := fc.getFileMetaByVolAddr(lackLoc); ok {
			continue
		}
		msg := fc.generatorLackFileLog(volID, lackLoc, liveVols)
		if lackLoc.Status == VolUnavailable {
			msg = msg + fmt.Sprintf(" ,But volLocationStatus :%v  So Can't do Replicat Task",
				lackLoc.Status)
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

func (fc *FileInCore) generatorLackFileLog(volID uint64, lackLoc *DataReplica, liveLocs []*DataReplica) (log string) {
	allNodeInfoAddrs := make([]string, 0)
	allLiveLocsAddrs := make([]string, len(liveLocs))
	for _, fm := range fc.Metas {
		allNodeInfoAddrs = append(allNodeInfoAddrs, fm.getLocationAddr())
	}
	for _, volLoc := range liveLocs {
		allLiveLocsAddrs = append(allLiveLocsAddrs, volLoc.Addr)
	}

	log = fmt.Sprintf(GetLackFileNodeTaskErr+"check volID:%v  File:%v  "+
		"on Node:%v  currPtr[%p] AllLiveLocsPtr:%v  AllLiveLocsAddr:%v  "+
		"allNodeInfosPtr:%v  AllNodeInfoAddrs:%v  Not Exsit,So Must Repair it  ",
		volID, fc.Name, lackLoc.Addr, lackLoc, liveLocs, allLiveLocsAddrs, fc.Metas,
		allNodeInfoAddrs)

	return
}

func (fc *FileInCore) generatorReplicateFileTask(volID uint64, badLoc *DataReplica, liveLocs []*DataReplica) (t *proto.AdminTask) {
	return proto.NewAdminTask(proto.OpReplicateFile, badLoc.Addr, nil)
}
