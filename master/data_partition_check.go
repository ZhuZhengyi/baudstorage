package master

import (
	"fmt"
	"github.com/tiglabs/baudstorage/util/log"
	"time"
)

func (partition *DataPartition) checkStatus(needLog bool, dpTimeOutSec int64) {
	partition.Lock()
	defer partition.Unlock()
	liveReplicas := partition.getLiveReplicasByPersistenceHosts(dpTimeOutSec)
	switch len(liveReplicas) {
	case (int)(partition.ReplicaNum):
		partition.Status = DataPartitionReadOnly
		if partition.checkReplicaStatusOnLiveNode(liveReplicas) == true {
			partition.Status = DataPartitionReadWrite
		}
	default:
		partition.Status = DataPartitionReadOnly
	}
	if needLog == true {
		msg := fmt.Sprintf("action[checkStatus],partitionID:%v  replicaNum:%v  liveReplicas:%v   Status:%v  RocksDBHost:%v ",
			partition.PartitionID, partition.ReplicaNum, len(liveReplicas), partition.Status, partition.PersistenceHosts)
		log.LogInfo(msg)
	}
}

func (partition *DataPartition) checkReplicaStatusOnLiveNode(liveReplicas []*DataReplica) (equal bool) {
	for _, replica := range liveReplicas {
		if replica.Status != DataPartitionReadWrite {
			return
		}
	}

	return true
}

func (partition *DataPartition) checkReplicaStatus(timeOutSec int64) {
	partition.Lock()
	defer partition.Unlock()
	for _, replica := range partition.Replicas {
		replica.IsLive(timeOutSec)
	}

}

func (partition *DataPartition) checkMiss(clusterID string, dataPartitionMissSec, dataPartitionWarnInterval int64) {
	partition.Lock()
	defer partition.Unlock()
	for _, replica := range partition.Replicas {
		if partition.isInPersistenceHosts(replica.Addr) && replica.CheckMiss(dataPartitionMissSec) == true && partition.needWarnMissDataPartition(replica.Addr, dataPartitionWarnInterval) {
			dataNode := replica.GetReplicaNode()
			var (
				lastReportTime time.Time
			)
			isActive := true
			if dataNode != nil {
				lastReportTime = dataNode.ReportTime
				isActive = dataNode.isActive
			}
			msg := fmt.Sprintf("action[checkMissErr], paritionID:%v  on Node:%v  "+
				"miss time > :%v  lastRepostTime:%v   dnodeLastReportTime:%v  nodeisActive:%v So Migrate", partition.PartitionID,
				replica.Addr, dataPartitionMissSec, replica.ReportTime, lastReportTime, isActive)
			Warn(clusterID, msg)
		}
	}

	for _, addr := range partition.PersistenceHosts {
		if partition.missDataPartition(addr) == true && partition.needWarnMissDataPartition(addr, dataPartitionWarnInterval) {
			msg := fmt.Sprintf("action[checkMissErr], partitionID:%v  on Node:%v  "+
				"miss time  > :%v  but server not exsit So Migrate", partition.PartitionID, addr, dataPartitionMissSec)
			Warn(clusterID, msg)
		}
	}
}

func (partition *DataPartition) needWarnMissDataPartition(addr string, dataPartitionWarnInterval int64) (isWarn bool) {
	warnTime, ok := partition.MissNodes[addr]
	if !ok {
		partition.MissNodes[addr] = time.Now().Unix()
		isWarn = true
	} else {
		if time.Now().Unix()-warnTime > dataPartitionWarnInterval {
			isWarn = true
			partition.MissNodes[addr] = time.Now().Unix()
		}
	}

	return
}

func (partition *DataPartition) missDataPartition(addr string) (isMiss bool) {
	_, ok := partition.IsInReplicas(addr)

	if ok == false {
		isMiss = true
	}

	return
}

func (partition *DataPartition) checkDiskError() (diskErrorAddrs []string) {
	diskErrorAddrs = make([]string, 0)
	partition.Lock()
	for _, addr := range partition.PersistenceHosts {
		replica, ok := partition.IsInReplicas(addr)
		if !ok {
			continue
		}
		if replica.Status == DataPartitionUnavailable {
			diskErrorAddrs = append(diskErrorAddrs, addr)
		}
	}

	if len(diskErrorAddrs) != (int)(partition.ReplicaNum) && len(diskErrorAddrs) > 0 {
		partition.Status = DataPartitionReadOnly
	}
	partition.Unlock()

	for _, diskAddr := range diskErrorAddrs {
		msg := fmt.Sprintf("action[%v],partitionID:%v  On :%v  Disk Error,So Remove it From RocksDBHost", CheckDataPartitionDiskErrorErr, partition.PartitionID, diskAddr)
		log.LogError(msg)
	}

	return
}
