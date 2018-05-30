package master

import (
	"fmt"
	"github.com/tiglabs/baudstorage/util/log"
	"time"
)

func (vg *VolGroup) checkStatus(needLog bool, volTimeOutSec int64) {
	vg.Lock()
	defer vg.Unlock()
	liveVolLocs := vg.getLiveVolsByPersistenceHosts(volTimeOutSec)
	switch len(liveVolLocs) {
	case (int)(vg.ReplicaNum):
		vg.Status = VolReadOnly
		if vg.checkVolLocStatusOnLiveNode(liveVolLocs) == true {
			vg.Status = VolReadWrite
		}
	default:
		vg.Status = VolReadOnly
	}
	if needLog == true {
		msg := fmt.Sprintf("action[checkStatus],volID:%v  goal:%v  liveLocation:%v   VolStatus:%v  RocksDBHost:%v ",
			vg.VolID, vg.ReplicaNum, len(liveVolLocs), vg.Status, vg.PersistenceHosts)
		log.LogInfo(msg)
	}
}

func (vg *VolGroup) checkVolLocStatusOnLiveNode(liveLocs []*Vol) (volEqual bool) {
	for _, volLoc := range liveLocs {
		if volLoc.status != VolReadWrite {
			return
		}
	}

	return true
}

func (vg *VolGroup) checkLocationStatus(volTimeOutSec int64) {
	vg.Lock()
	defer vg.Unlock()
	for _, volLoc := range vg.Locations {
		volLoc.IsLive(volTimeOutSec)
	}

}

func (vg *VolGroup) checkVolGroupMiss(volMissSec, volWarnInterval int64) {
	vg.Lock()
	defer vg.Unlock()
	for _, volLoc := range vg.Locations {
		if _, ok := vg.IsInVolLocs(volLoc.Addr); ok && volLoc.CheckVolMiss(volMissSec) == true && vg.needWarnMissVol(volLoc.Addr, volWarnInterval) {
			dataNode := volLoc.GetVolLocationNode()
			var (
				lastReportTime time.Time
			)
			isActive := true
			if dataNode != nil {
				lastReportTime = dataNode.reportTime
				isActive = dataNode.isActive
			}
			msg := fmt.Sprintf("action[checkVolMissErr], vol:%v  on Node:%v  "+
				"miss time > :%v  vlocLastRepostTime:%v   dnodeLastReportTime:%v  nodeisActive:%v So Migrate", vg.VolID,
				volLoc.Addr, volMissSec, volLoc.ReportTime, lastReportTime, isActive)
			log.LogError(msg)
		}
	}

	for _, addr := range vg.PersistenceHosts {
		if vg.missVol(addr) == true && vg.needWarnMissVol(addr, volWarnInterval) {
			msg := fmt.Sprintf("action[checkVolMissErr], vol:%v  on Node:%v  "+
				"miss time  > :%v  but server not exsit So Migrate", vg.VolID, addr, volMissSec)
			log.LogError(msg)
		}
	}
}

func (vg *VolGroup) needWarnMissVol(addr string, volWarnInterval int64) (isWarn bool) {
	warnTime, ok := vg.MissNodes[addr]
	if !ok {
		vg.MissNodes[addr] = time.Now().Unix()
		isWarn = true
	} else {
		if time.Now().Unix()-warnTime > volWarnInterval {
			isWarn = true
			vg.MissNodes[addr] = time.Now().Unix()
		}
	}

	return
}

func (vg *VolGroup) missVol(addr string) (isMiss bool) {
	_, addrIsInLocs := vg.IsInVolLocs(addr)

	if addrIsInLocs == false {
		isMiss = true
	}

	return
}

func (vg *VolGroup) checkVolDiskError() (volDiskErrorAddrs []string) {
	volDiskErrorAddrs = make([]string, 0)
	vg.Lock()
	for _, addr := range vg.PersistenceHosts {
		volLoc, ok := vg.IsInVolLocs(addr)
		if !ok {
			continue
		}
		if volLoc.status == VolUnavailable {
			volDiskErrorAddrs = append(volDiskErrorAddrs, addr)
		}
	}

	if len(volDiskErrorAddrs) != (int)(vg.ReplicaNum) && len(volDiskErrorAddrs) > 0 {
		vg.Status = VolReadOnly
	}
	vg.Unlock()

	for _, diskAddr := range volDiskErrorAddrs {
		msg := fmt.Sprintf("action[%v],vol:%v  On :%v  Disk Error,So Remove it From RocksDBHost", CheckVolDiskErrorErr, vg.VolID, diskAddr)
		log.LogError(msg)
	}

	return
}
