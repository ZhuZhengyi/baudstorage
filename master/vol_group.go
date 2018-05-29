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
	vg.MissNodes = make(map[string]int64)
	return
}

func (vg *VolGroup) addMember(vl *Vol) {
	vg.Lock()
	defer vg.Unlock()
	for _, vol := range vg.locations {
		if vl.addr == vol.addr {
			return
		}
	}
	vg.locations = append(vg.locations, vl)
}

func (vg *VolGroup) checkBadStatus() {

}

func (vg *VolGroup) generateCreateVolGroupTasks() (tasks []*proto.AdminTask) {
	tasks = make([]*proto.AdminTask, 0)
	for _, addr := range vg.PersistenceHosts {
		tasks = append(tasks, proto.NewAdminTask(proto.OpCreateVol, addr, newCreateVolRequest(vg.volType, vg.VolID)))
	}
	return
}

func (vg *VolGroup) hasMissOne() (err error) {
	availPersistenceHostLen := len(vg.PersistenceHosts)
	if availPersistenceHostLen <= (int)(vg.replicaNum)-1 {
		log.LogError(fmt.Sprintf("action[%v],volID:%v,err:%v",
			"hasMissOne", vg.VolID, VolReplicationHasMissOneError))
		err = VolReplicationHasMissOneError
	}
	return
}

func (vg *VolGroup) canOffLine(offlineAddr string) (err error) {
	msg := fmt.Sprintf("action[canOffLine],vol:%v  RocksDBHost:%v  offLine:%v ",
		vg.VolID, vg.PersistenceHosts, offlineAddr)
	liveLocs := vg.getLiveVols(DefaultVolTimeOutSec)
	if len(liveLocs) < 2 {
		msg = fmt.Sprintf(msg+" err:%v  liveLocs:%v ", CannotOffLineErr, len(liveLocs))
		log.LogError(msg)
		err = fmt.Errorf(msg)
	}

	return
}

func (vg *VolGroup) generatorVolOffLineLog(offlineAddr string) (msg string) {
	msg = fmt.Sprintf("action[GeneratorVolOffLineLogInfo],vol:%v  offlineaddr:%v  ",
		vg.VolID, offlineAddr)
	vols := vg.GetAvailableVols()
	for i := 0; i < len(vols); i++ {
		vol := vols[i]
		msg += fmt.Sprintf(" addr:%v  volStatus:%v  FileCount :%v ", vol.addr,
			vol.status, vol.FileCount)
	}
	log.LogWarn(msg)

	return
}

/*获取该副本目前有效的node,即Node在汇报心跳正常，并且该Node不是unavailable*/
func (vg *VolGroup) GetAvailableVols() (vols []*Vol) {
	vols = make([]*Vol, 0)
	for i := 0; i < len(vg.locations); i++ {
		vol := vg.locations[i]
		if vol.CheckLocIsAvailContainsDiskError() == true && vg.isInPersistenceHosts(vol.addr) == true {
			vols = append(vols, vol)
		}
	}

	return
}

func (vg *VolGroup) volOffLineInMem(addr string) {
	delIndex := -1
	var loc *Vol
	for i := 0; i < len(vg.locations); i++ {
		vol := vg.locations[i]
		if vol.addr == addr {
			loc = vol
			delIndex = i
			break
		}
	}
	msg := fmt.Sprintf("action[VolOffLineInMem],vol:%v  on Node:%v  OffLine,the node is in volLocs:%v", vg.VolID, addr, loc != nil)
	log.LogDebug(msg)
	if loc == nil {
		return
	}

	for _, fc := range vg.FileInCoreMap {
		fc.deleteFileInNode(vg.VolID, loc)
	}
	vg.DeleteVolByIndex(delIndex)

	return
}

func (vg *VolGroup) DeleteVolByIndex(index int) {
	var locArr []string
	for _, loc := range vg.locations {
		locArr = append(locArr, loc.addr)
	}
	msg := fmt.Sprintf("DeleteVolByIndex vol:%v  index:%v  locations :%v ", vg.VolID, index, locArr)
	log.LogInfo(msg)
	volLocsAfter := vg.locations[index+1:]
	vg.locations = vg.locations[:index]
	vg.locations = append(vg.locations, volLocsAfter...)
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
		tasks = append(tasks, proto.NewAdminTask(proto.OpLoadVol, vol.addr, newLoadVolMetricRequest(vg.volType, vg.VolID)))
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

func (vg *VolGroup) checkReplicaNum(c *Cluster, nsName string) {
	vg.Lock()
	defer vg.Unlock()
	if int(vg.replicaNum) != len(vg.PersistenceHosts) {
		orgGoal := vg.replicaNum
		vg.replicaNum = (uint8)(len(vg.PersistenceHosts))
		vg.UpdateVolHosts(c, nsName)
		msg := fmt.Sprintf("FIX VOL GOAL,vol:%v orgGoal:%v volHOST:%v",
			vg.VolID, orgGoal, vg.VolHostsToString())
		log.LogWarn(msg)
	}
}

func (vg *VolGroup) VolHostsToString() (hosts string) {
	return strings.Join(vg.PersistenceHosts, UnderlineSeparator)
}

func (vg *VolGroup) UpdateVolHosts(c *Cluster, nsName string) error {
	return c.syncUpdateVolGroup(nsName, vg)
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

			t = proto.NewAdminTask(proto.OpCreateVol, addr, newCreateVolRequest(vg.volType, vg.VolID))
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

func (vg *VolGroup) checkAndRemoveMissVol(addr string) {
	if _, ok := vg.MissNodes[addr]; ok {
		delete(vg.MissNodes, addr)
	}
}

func (vg *VolGroup) LoadFile(dataNode *DataNode, resp *proto.LoadVolResponse) {
	vg.Lock()
	defer vg.Unlock()

	index, err := vg.getVolLocationIndex(dataNode.HttpAddr)
	if err != nil {
		msg := fmt.Sprintf("LoadFile volID:%v  on Node:%v  don't report :%v ", vg.VolID, dataNode.HttpAddr, err)
		log.LogWarn(msg)
		return
	}
	volLoc := vg.locations[index]
	volLoc.LoadVolIsResponse = true
	for _, vf := range resp.VolSnapshot {
		if vf == nil {
			continue
		}
		fc, ok := vg.FileInCoreMap[vf.Name]
		if !ok {
			fc = NewFileInCore(vf.Name)
			vg.FileInCoreMap[vf.Name] = fc
		}
		fc.updateFileInCore(vg.VolID, vf, volLoc, index)
	}
}

func (vg *VolGroup) getVolLocationIndex(addr string) (volLocIndex int, err error) {
	for volLocIndex = 0; volLocIndex < len(vg.locations); volLocIndex++ {
		volLoc := vg.locations[volLocIndex]
		if volLoc.addr == addr {
			return
		}
	}
	log.LogError(fmt.Sprintf("action[getVolLocationIndex],volID:%v,location:%v,err:%v",
		vg.VolID, addr, VolLocationNotFound))
	return -1, VolLocationNotFound
}

func (vg *VolGroup) DeleteFileOnNode(delAddr, FileID string) {
	vg.Lock()
	defer vg.Unlock()
	fc, ok := vg.FileInCoreMap[FileID]
	if !ok || fc.MarkDel == false {
		return
	}
	volLoc, err := vg.getVolLocation(delAddr)
	if err != nil {
		return
	}
	fc.deleteFileInNode(vg.VolID, volLoc)

	msg := fmt.Sprintf("vol:%v  File:%v  on node:%v  delete success",
		vg.VolID, fc.Name, delAddr)
	log.LogInfo(msg)

	if len(fc.Metas) == 0 {
		delete(vg.FileInCoreMap, fc.Name)
		msg = fmt.Sprintf("vol:%v  File:%v  delete success on allNode", vg.VolID, fc.Name)
		log.LogInfo(msg)
	}

	return
}

func (vg *VolGroup) removeVolHosts(removeAddr string, c *Cluster, nsName string) (err error) {
	orgGoal := len(vg.PersistenceHosts)
	orgVolHosts := make([]string, len(vg.PersistenceHosts))
	copy(orgVolHosts, vg.PersistenceHosts)

	if ok := vg.removeVolHostOnUnderStore(removeAddr); !ok {
		return
	}
	vg.replicaNum = (uint8)(len(vg.PersistenceHosts))
	if err = vg.UpdateVolHosts(c, nsName); err != nil {
		vg.replicaNum = (uint8)(orgGoal)
		vg.PersistenceHosts = orgVolHosts
	}

	msg := fmt.Sprintf("RemoveVolHostsInfo  vol:%v  Delete host:%v  on PersistenceHosts:%v ",
		vg.VolID, removeAddr, vg.PersistenceHosts)
	log.LogDebug(msg)

	return
}

func (vg *VolGroup) removeVolHostOnUnderStore(removeAddr string) (ok bool) {
	for index, addr := range vg.PersistenceHosts {
		if addr == removeAddr {
			after := vg.PersistenceHosts[index+1:]
			vg.PersistenceHosts = vg.PersistenceHosts[:index]
			vg.PersistenceHosts = append(vg.PersistenceHosts, after...)
			ok = true
			break
		}
	}

	return
}

func (vg *VolGroup) addVolHosts(addAddr string, c *Cluster, nsName string) (err error) {
	orgVolHosts := make([]string, len(vg.PersistenceHosts))
	orgGoal := len(vg.PersistenceHosts)
	copy(orgVolHosts, vg.PersistenceHosts)
	for _, addr := range vg.PersistenceHosts {
		if addr == addAddr {
			return
		}
	}
	vg.PersistenceHosts = append(vg.PersistenceHosts, addAddr)
	vg.replicaNum = uint8(len(vg.PersistenceHosts))
	if err = vg.UpdateVolHosts(c, nsName); err != nil {
		vg.PersistenceHosts = orgVolHosts
		vg.replicaNum = uint8(orgGoal)
		return
	}
	msg := fmt.Sprintf(" AddVolHostsInfo vol:%v  Add host:%v  PersistenceHosts:%v ",
		vg.VolID, addAddr, vg.PersistenceHosts)
	log.LogDebug(msg)
	return
}

func (vg *VolGroup) UpdateVol(vr *proto.VolReport, dataNode *DataNode) {
	vg.Lock()
	volLoc, err := vg.getVolLocation(dataNode.HttpAddr)
	vg.Unlock()

	if err != nil && !vg.isInPersistenceHosts(dataNode.HttpAddr) {
		return
	}
	if err != nil && vg.isInPersistenceHosts(dataNode.HttpAddr) {
		volLoc = NewVol(dataNode)
		vg.addMember(volLoc)
	}
	volLoc.status = (uint8)(vr.VolStatus)
	volLoc.Total = vr.Total
	volLoc.Used = vr.Used
	volLoc.SetVolAlive()
	vg.Lock()
	vg.checkAndRemoveMissVol(dataNode.HttpAddr)
	vg.Unlock()
}
