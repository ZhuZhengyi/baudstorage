package master

import (
	"github.com/tiglabs/baudstorage/proto"
)

/*check File: recover File,if File lack or timeOut report or crc bad*/
func (partition *DataPartition) checkFile(isRecoverFlag bool, clusterID string) (tasks []*proto.AdminTask) {
	tasks = make([]*proto.AdminTask, 0)
	partition.Lock()
	defer partition.Unlock()
	liveReplicas := partition.getLiveReplicas(DefaultDataPartitionTimeOutSec)
	if len(liveReplicas) == 0 {
		return
	}

	switch partition.PartitionType {
	case proto.ExtentVol:
		partition.checkExtentFile(liveReplicas, isRecoverFlag, clusterID)
	case proto.TinyVol:
		partition.checkChunkFile(liveReplicas, clusterID)
	}

	return
}

func (partition *DataPartition) checkChunkFile(liveReplicas []*DataReplica, clusterID string) (tasks []*proto.AdminTask) {
	for _, fc := range partition.FileInCoreMap {
		tasks = append(tasks, fc.generateFileCrcTask(partition.PartitionID, liveReplicas, ChunkDataPartition, clusterID)...)
	}
	return
}

func (partition *DataPartition) checkExtentFile(liveReplicas []*DataReplica, isRecoverFlag bool, clusterID string) (tasks []*proto.AdminTask) {
	for _, fc := range partition.FileInCoreMap {
		tasks = append(tasks, fc.generateFileCrcTask(partition.PartitionID, liveReplicas, ExtentDataPartition, clusterID)...)
	}
	return
}
