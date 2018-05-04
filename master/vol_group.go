package master

import (
	"fmt"
	"github.com/tiglabs/baudstorage/proto"
	"github.com/tiglabs/baudstorage/util/log"
	"strings"
	"sync"
	"time"
)

type VolGroup struct {
	VolID            uint64
	LastLoadTime     int64
	replicaNum       uint8
	status           uint8
	isRecover        bool
	locations        []*Vol
	volType          string
	PersistenceHosts []string
	sync.Mutex

	FileInCoreMap map[string]*FileInCore
	MissNodes     map[string]int64
}

func newVolGroup(volID uint64, replicaNum uint8) (vg *VolGroup) {
	vg = new(VolGroup)
	vg.replicaNum = replicaNum
	vg.VolID = volID
	vg.PersistenceHosts = make([]string, 0)
	vg.locations = make([]*Vol, 0)
	vg.FileInCoreMap = make(map[string]*FileInCore, 0)
	return
}

func (vg *VolGroup) addMember(vl *Vol) {

}

func (vg *VolGroup) checkBadStatus() {

}

func (vg *VolGroup) ChooseTargetHosts(c *Cluster) (err error) {
	var (
		addrs []string
	)
	vg.Lock()
	defer vg.Unlock()
	if addrs, err = c.getAvailDataNodeHosts(vg.PersistenceHosts, int(vg.replicaNum)); err != nil {
		return
	}
	vg.PersistenceHosts = append(vg.PersistenceHosts, addrs...)

	if len(vg.PersistenceHosts) != (int)(vg.replicaNum) {
		err = fmt.Errorf("no have enough volhost exsited")
		return
	}
	log.LogDebug(fmt.Sprintf("action[ChooseTargetHosts],volID:%v,PersistenceHosts:%v",
		vg.VolID, vg.PersistenceHosts))
	return
}

func (vg *VolGroup) generateCreateVolGroupTasks() (tasks []*proto.AdminTask) {
	tasks = make([]*proto.AdminTask, 0)
	for _, addr := range vg.PersistenceHosts {
		tasks = append(tasks, proto.NewAdminTask(OpCreateVol, addr, newCreateVolRequest(vg.volType, vg.VolID)))
	}
	return
}

/*vol的某个副本磁盘坏的情况下触发vol迁移，非常重要*/
func (vg *VolGroup) volOffLine(offlineAddr string, sourceFunc string) {

}

func (vg *VolGroup) generateLoadVolTasks() (tasks []*proto.AdminTask) {

	vg.Lock()
	defer vg.Unlock()
	for _, addr := range vg.PersistenceHosts {
		vol, err := vg.getVolLocation(addr)
		if err != nil || vol.IsLive(DefaultVolTimeOutSec) == false {
			continue
		}
		vol.LoadVolIsResponse = false
		tasks = append(tasks, proto.NewAdminTask(OpLoadVol, vol.addr, newLoadVolMetricRequest(vg.volType, vg.VolID)))
	}
	vg.LastLoadTime = time.Now().Unix()
	return
}

func (vg *VolGroup) getVolLocation(addr string) (vol *Vol, err error) {
	for index := 0; index < len(vg.locations); index++ {
		vol = vg.locations[index]
		if vol.addr == addr {
			return
		}
	}
	log.LogError(fmt.Sprintf("action[getVolLocation],volID:%v,locations:%v,err:%v",
		vg.VolID, addr, VolLocationNotFound))
	return nil, VolLocationNotFound
}

func (vg *VolGroup) convertToVolResponse() (vr *VolResponse) {
	vr = new(VolResponse)
	vg.Lock()
	defer vg.Unlock()
	vr.VolID = vg.VolID
	vr.Status = vg.status
	vr.ReplicaNum = vg.replicaNum
	vr.Hosts = make([]string, len(vg.PersistenceHosts))
	copy(vr.Hosts, vg.PersistenceHosts)
	return
}

func (vg *VolGroup) checkLoadVolResponse(volTimeOutSec int64) (isResponse bool) {
	vg.Lock()
	defer vg.Unlock()
	for _, addr := range vg.PersistenceHosts {
		volLoc, err := vg.getVolLocation(addr)
		if err != nil {
			return
		}
		loadVolTime := time.Now().Unix() - vg.LastLoadTime
		if volLoc.LoadVolIsResponse == false && loadVolTime > LoadVolWaitTime {
			msg := fmt.Sprintf("action[checkLoadVolResponse], volId:%v on Node:%v no response, spent time %v s", vg.VolID, addr, loadVolTime)
			log.LogWarn(msg)
			return
		}
		if volLoc.IsLive(volTimeOutSec) == false || volLoc.LoadVolIsResponse == false {
			return
		}
	}
	isResponse = true

	return
}

func (vg *VolGroup) getVolLocationByIndex(index uint8) (volLoc *Vol) {
	return vg.locations[int(index)]
}

func (vg *VolGroup) getFileCount() {
	var msg string
	needDelFiles := make([]string, 0)
	vg.Lock()
	defer vg.Unlock()
	for _, volLoc := range vg.locations {
		volLoc.FileCount = 0
	}
	for _, fc := range vg.FileInCoreMap {
		if fc.MarkDel == true {
			continue
		}
		if len(fc.Metas) == 0 {
			needDelFiles = append(needDelFiles, fc.Name)
		}
		for _, vfNode := range fc.Metas {
			volLoc := vg.getVolLocationByIndex(vfNode.LocIndex)
			volLoc.FileCount++
		}

	}

	for _, vfName := range needDelFiles {
		delete(vg.FileInCoreMap, vfName)
	}

	for _, volLoc := range vg.locations {
		msg = fmt.Sprintf(GetVolLocationFileCountInfo+"vol:%v  volLocation:%v  FileCount:%v  "+
			"NodeIsActive:%v  VlocIsActive:%v  .VolStatusOnNode:%v ", vg.VolID, volLoc.addr, volLoc.FileCount,
			volLoc.GetVolLocationNode().isActive, volLoc.IsActive(DefaultVolTimeOutSec), volLoc.status)
		log.LogInfo(msg)
	}

}

func (vg *VolGroup) ReleaseVol() {
	vg.Lock()
	defer vg.Unlock()
	liveLocs := vg.getLiveVolsByPersistenceHosts(DefaultVolTimeOutSec)
	for _, volLoc := range liveLocs {
		volLoc.LoadVolIsResponse = false
	}
	for name, fc := range vg.FileInCoreMap {
		fc.Metas = nil
		delete(vg.FileInCoreMap, name)
	}
	vg.FileInCoreMap = make(map[string]*FileInCore, 0)

}

func (vg *VolGroup) IsInVolLocs(host string) (volLoc *Vol, ok bool) {
	for _, volLoc = range vg.locations {
		if volLoc.addr == host {
			ok = true
			break
		}
	}
	return
}

func (vg *VolGroup) checkReplicaNum() {
	vg.Lock()
	defer vg.Unlock()
	if int(vg.replicaNum) != len(vg.PersistenceHosts) {
		orgGoal := vg.replicaNum
		vg.replicaNum = (uint8)(len(vg.PersistenceHosts))
		vg.UpdateVolHosts()
		msg := fmt.Sprintf("FIX VOL GOAL,vol:%v orgGoal:%v volHOST:%v",
			vg.VolID, orgGoal, vg.VolHostsToString())
		log.LogWarn(msg)
	}
}

func (vg *VolGroup) VolHostsToString() (hosts string) {
	return strings.Join(vg.PersistenceHosts, UnderlineSeparator)
}

func (vg *VolGroup) UpdateVolHosts() error {
	return vg.PutVolHosts()
}

func (vg *VolGroup) PutVolHosts() (err error) {

	//todo sync vol hosts by raft
	return
}

func (vg *VolGroup) setVolToNormal() {
	vg.Lock()
	defer vg.Unlock()
	vg.isRecover = false
}

func (vg *VolGroup) isInPersistenceHosts(volAddr string) (ok bool) {
	for _, addr := range vg.PersistenceHosts {
		if addr == volAddr {
			ok = true
			break
		}
	}

	return
}

func (vg *VolGroup) checkVolReplicationTask() (tasks []*proto.AdminTask) {
	var msg string
	tasks = make([]*proto.AdminTask, 0)
	if excessAddr, excessErr := vg.deleteExcessReplication(); excessErr != nil {
		msg = fmt.Sprintf("action[%v], vol:%v  Excess Replication"+
			" On :%v  Err:%v  rocksDBRecords:%v  so please Delete Vol BY SHOUGONG",
			DeleteExcessReplicationErr, vg.VolID, excessAddr, excessErr.Error(), vg.PersistenceHosts)
		log.LogWarn(msg)
	}
	if vg.status == VolReadWrite {
		return
	}
	if lackTask, lackAddr, lackErr := vg.addLackReplication(); lackErr != nil {
		tasks = append(tasks, lackTask)
		msg = fmt.Sprintf("action[%v], vol:%v  Lack Replication"+
			" On :%v  Err:%v  rocksDBRecords:%v  NewTask Create Vol",
			AddLackReplicationErr, vg.VolID, lackAddr, lackErr.Error(), vg.PersistenceHosts)
		log.LogWarn(msg)
	} else {
		vg.setVolToNormal()
	}

	return
}

/*delete vol excess replication ,range all volLocs
if volLocation not in volRocksDBHosts then generator task to delete volume*/
func (vg *VolGroup) deleteExcessReplication() (excessAddr string, err error) {
	vg.Lock()
	defer vg.Unlock()
	for i := 0; i < len(vg.locations); i++ {
		volLoc := vg.locations[i]
		if ok := vg.isInPersistenceHosts(volLoc.addr); !ok {
			excessAddr = volLoc.addr
			log.LogError(fmt.Sprintf("action[deleteExcessReplication],volID:%v,has excess replication:%v",
				vg.VolID, excessAddr))
			err = VolReplicationExcessError
			break
		}
	}

	return
}

/*add vol lack replication,range all volRocksDBHost if volHosts not in volLocations,
then generator a task to OpRecoverCreateVol to a new Node*/
func (vg *VolGroup) addLackReplication() (t *proto.AdminTask, lackAddr string, err error) {
	vg.Lock()
	for _, addr := range vg.PersistenceHosts {
		if ok := vg.isInPersistenceHosts(addr); !ok {
			log.LogError(fmt.Sprintf("action[addLackReplication],volID:%v lack replication:%v",
				vg.VolID, addr))
			err = VolReplicationLackError
			lackAddr = addr

			t = proto.NewAdminTask(OpCreateVol, addr, newCreateVolRequest(vg.volType, vg.VolID))
			vg.isRecover = true
			break
		}
	}
	vg.Unlock()

	return
}

func (vg *VolGroup) getLiveVols(volTimeOutSec int64) (vols []*Vol) {
	vols = make([]*Vol, 0)
	for i := 0; i < len(vg.locations); i++ {
		vol := vg.locations[i]
		if vol.IsLive(volTimeOutSec) == true && vg.isInPersistenceHosts(vol.addr) == true {
			vols = append(vols, vol)
		}
	}

	return
}

func (vg *VolGroup) getLiveVolsByPersistenceHosts(volTimeOutSec int64) (vols []*Vol) {
	vols = make([]*Vol, 0)
	for _, host := range vg.PersistenceHosts {
		volLoc, ok := vg.IsInVolLocs(host)
		if !ok {
			continue
		}
		if volLoc.IsLive(volTimeOutSec) == true {
			vols = append(vols, volLoc)
		}
	}

	return
}
