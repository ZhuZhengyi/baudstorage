package master

import (
	"fmt"
	"github.com/tiglabs/baudstorage/util/log"
	"time"
)

func (partition *DataPartition) checkStatus(needLog bool, volTimeOutSec int64) {
	partition.Lock()
	defer partition.Unlock()
	liveVolLocs := partition.getLiveVolsByPersistenceHosts(volTimeOutSec)
	switch len(liveVolLocs) {
	case (int)(partition.ReplicaNum):
		partition.Status = VolReadOnly
		if partition.checkVolLocStatusOnLiveNode(liveVolLocs) == true {
			partition.Status = VolReadWrite
		}
	default:
		partition.Status = VolReadOnly
	}
	if needLog == true {
		msg := fmt.Sprintf("action[checkStatus],volID:%v  goal:%v  liveLocation:%v   VolStatus:%v  RocksDBHost:%v ",
			partition.PartitionID, partition.ReplicaNum, len(liveVolLocs), partition.Status, partition.PersistenceHosts)
		log.LogInfo(msg)
	}
}

func (partition *DataPartition) checkVolLocStatusOnLiveNode(liveLocs []*DataReplica) (volEqual bool) {
	for _, volLoc := range liveLocs {
		if volLoc.Status != VolReadWrite {
			return
		}
	}

	return true
}

func (partition *DataPartition) checkLocationStatus(volTimeOutSec int64) {
	partition.Lock()
	defer partition.Unlock()
	for _, volLoc := range partition.Locations {
		volLoc.IsLive(volTimeOutSec)
	}

}

func (partition *DataPartition) checkVolGroupMiss(clusterID string, volMissSec, volWarnInterval int64) {
	partition.Lock()
	defer partition.Unlock()
	for _, volLoc := range partition.Locations {
		if partition.isInPersistenceHosts(volLoc.Addr) && volLoc.CheckMiss(volMissSec) == true && partition.needWarnMissVol(volLoc.Addr, volWarnInterval) {
			dataNode := volLoc.GetVolLocationNode()
			var (
				lastReportTime time.Time
			)
			isActive := true
			if dataNode != nil {
				lastReportTime = dataNode.ReportTime
				isActive = dataNode.isActive
			}
			msg := fmt.Sprintf("action[checkVolMissErr], vol:%v  on Node:%v  "+
				"miss time > :%v  vlocLastRepostTime:%v   dnodeLastReportTime:%v  nodeisActive:%v So Migrate", partition.PartitionID,
				volLoc.Addr, volMissSec, volLoc.ReportTime, lastReportTime, isActive)
			Warn(clusterID, msg)
		}
	}

	for _, addr := range partition.PersistenceHosts {
		if partition.missVol(addr) == true && partition.needWarnMissVol(addr, volWarnInterval) {
			msg := fmt.Sprintf("action[checkVolMissErr], vol:%v  on Node:%v  "+
				"miss time  > :%v  but server not exsit So Migrate", partition.PartitionID, addr, volMissSec)
			Warn(clusterID, msg)
		}
	}
}

func (partition *DataPartition) needWarnMissVol(addr string, volWarnInterval int64) (isWarn bool) {
	warnTime, ok := partition.MissNodes[addr]
	if !ok {
		partition.MissNodes[addr] = time.Now().Unix()
		isWarn = true
	} else {
		if time.Now().Unix()-warnTime > volWarnInterval {
			isWarn = true
			partition.MissNodes[addr] = time.Now().Unix()
		}
	}

	return
}

func (partition *DataPartition) missVol(addr string) (isMiss bool) {
	_, addrIsInLocs := partition.IsInVolLocs(addr)

	if addrIsInLocs == false {
		isMiss = true
	}

	return
}

func (partition *DataPartition) checkVolDiskError() (volDiskErrorAddrs []string) {
	volDiskErrorAddrs = make([]string, 0)
	partition.Lock()
	for _, addr := range partition.PersistenceHosts {
		volLoc, ok := partition.IsInVolLocs(addr)
		if !ok {
			continue
		}
		if volLoc.Status == VolUnavailable {
			volDiskErrorAddrs = append(volDiskErrorAddrs, addr)
		}
	}

	if len(volDiskErrorAddrs) != (int)(partition.ReplicaNum) && len(volDiskErrorAddrs) > 0 {
		partition.Status = VolReadOnly
	}
	partition.Unlock()

	for _, diskAddr := range volDiskErrorAddrs {
		msg := fmt.Sprintf("action[%v],vol:%v  On :%v  Disk Error,So Remove it From RocksDBHost", CheckVolDiskErrorErr, partition.PartitionID, diskAddr)
		log.LogError(msg)
	}

	return
}
