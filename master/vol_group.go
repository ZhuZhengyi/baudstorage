package master

import (
	"fmt"
	"github.com/tiglabs/baudstorage/proto"
	"github.com/tiglabs/baudstorage/util/log"
	"strings"
	"sync"
	"time"
)

type DataPartition struct {
	PartitionID      uint64
	LastLoadTime     int64
	ReplicaNum       uint8
	Status           uint8
	isRecover        bool
	Locations        []*DataReplica
	PartitionType    string
	PersistenceHosts []string
	sync.Mutex

	FileInCoreMap map[string]*FileInCore
	MissNodes     map[string]int64
}

func newDataPartition(ID uint64, replicaNum uint8, partitionType string) (partition *DataPartition) {
	partition = new(DataPartition)
	partition.ReplicaNum = replicaNum
	partition.PartitionID = ID
	partition.PartitionType = partitionType
	partition.PersistenceHosts = make([]string, 0)
	partition.Locations = make([]*DataReplica, 0)
	partition.FileInCoreMap = make(map[string]*FileInCore, 0)
	partition.MissNodes = make(map[string]int64)
	return
}

func (partition *DataPartition) addMember(replica *DataReplica) {
	partition.Lock()
	defer partition.Unlock()
	for _, vol := range partition.Locations {
		if replica.Addr == vol.Addr {
			return
		}
	}
	partition.Locations = append(partition.Locations, replica)
}

func (partition *DataPartition) checkBadStatus() {

}

func (partition *DataPartition) generateCreateDataPartitionTasks() (tasks []*proto.AdminTask) {
	tasks = make([]*proto.AdminTask, 0)
	for _, addr := range partition.PersistenceHosts {
		t := proto.NewAdminTask(proto.OpCreateVol, addr, newCreateVolRequest(partition.PartitionType, partition.PartitionID))
		t.ID = fmt.Sprintf("%v_volID[%v]", t.ID, partition.PartitionID)
		tasks = append(tasks, t)
	}
	return
}

func (partition *DataPartition) hasMissOne() (err error) {
	availPersistenceHostLen := len(partition.PersistenceHosts)
	if availPersistenceHostLen <= (int)(partition.ReplicaNum)-1 {
		log.LogError(fmt.Sprintf("action[%v],volID:%v,err:%v",
			"hasMissOne", partition.PartitionID, DataReplicaHasMissOneError))
		err = DataReplicaHasMissOneError
	}
	return
}

func (partition *DataPartition) canOffLine(offlineAddr string) (err error) {
	msg := fmt.Sprintf("action[canOffLine],vol:%v  RocksDBHost:%v  offLine:%v ",
		partition.PartitionID, partition.PersistenceHosts, offlineAddr)
	liveLocs := partition.getLiveVols(DefaultVolTimeOutSec)
	if len(liveLocs) < 2 {
		msg = fmt.Sprintf(msg+" err:%v  liveLocs:%v ", CannotOffLineErr, len(liveLocs))
		log.LogError(msg)
		err = fmt.Errorf(msg)
	}

	return
}

func (partition *DataPartition) generatorVolOffLineLog(offlineAddr string) (msg string) {
	msg = fmt.Sprintf("action[GeneratorVolOffLineLogInfo],vol:%v  offlineaddr:%v  ",
		partition.PartitionID, offlineAddr)
	vols := partition.GetAvailableVols()
	for i := 0; i < len(vols); i++ {
		vol := vols[i]
		msg += fmt.Sprintf(" addr:%v  volStatus:%v  FileCount :%v ", vol.Addr,
			vol.Status, vol.FileCount)
	}
	log.LogWarn(msg)

	return
}

/*获取该副本目前有效的node,即Node在汇报心跳正常，并且该Node不是unavailable*/
func (partition *DataPartition) GetAvailableVols() (vols []*DataReplica) {
	vols = make([]*DataReplica, 0)
	for i := 0; i < len(partition.Locations); i++ {
		vol := partition.Locations[i]
		if vol.CheckLocIsAvailContainsDiskError() == true && partition.isInPersistenceHosts(vol.Addr) == true {
			vols = append(vols, vol)
		}
	}

	return
}

func (partition *DataPartition) volOffLineInMem(addr string) {
	delIndex := -1
	var loc *DataReplica
	for i := 0; i < len(partition.Locations); i++ {
		vol := partition.Locations[i]
		if vol.Addr == addr {
			loc = vol
			delIndex = i
			break
		}
	}
	msg := fmt.Sprintf("action[VolOffLineInMem],vol:%v  on Node:%v  OffLine,the node is in volLocs:%v", partition.PartitionID, addr, loc != nil)
	log.LogDebug(msg)
	if loc == nil {
		return
	}

	for _, fc := range partition.FileInCoreMap {
		fc.deleteFileInNode(partition.PartitionID, loc)
	}
	partition.DeleteVolByIndex(delIndex)

	return
}

func (partition *DataPartition) DeleteVolByIndex(index int) {
	var locArr []string
	for _, loc := range partition.Locations {
		locArr = append(locArr, loc.Addr)
	}
	msg := fmt.Sprintf("DeleteVolByIndex vol:%v  index:%v  locations :%v ", partition.PartitionID, index, locArr)
	log.LogInfo(msg)
	volLocsAfter := partition.Locations[index+1:]
	partition.Locations = partition.Locations[:index]
	partition.Locations = append(partition.Locations, volLocsAfter...)
}

func (partition *DataPartition) generateLoadVolTasks() (tasks []*proto.AdminTask) {

	partition.Lock()
	defer partition.Unlock()
	for _, addr := range partition.PersistenceHosts {
		vol, err := partition.getVolLocation(addr)
		if err != nil || vol.IsLive(DefaultVolTimeOutSec) == false {
			continue
		}
		vol.LoadPartitionIsResponse = false
		t := proto.NewAdminTask(proto.OpLoadVol, vol.Addr, newLoadVolMetricRequest(partition.PartitionType, partition.PartitionID))
		t.ID = fmt.Sprintf("%v_volID[%v]", t.ID, partition.PartitionID)
		tasks = append(tasks, t)
	}
	partition.LastLoadTime = time.Now().Unix()
	return
}

func (partition *DataPartition) getVolLocation(addr string) (vol *DataReplica, err error) {
	for index := 0; index < len(partition.Locations); index++ {
		vol = partition.Locations[index]
		if vol.Addr == addr {
			return
		}
	}
	log.LogError(fmt.Sprintf("action[getVolLocation],volID:%v,locations:%v,err:%v",
		partition.PartitionID, addr, DataReplicaNotFound))
	return nil, DataReplicaNotFound
}

func (partition *DataPartition) convertToVolResponse() (vr *VolResponse) {
	vr = new(VolResponse)
	partition.Lock()
	defer partition.Unlock()
	vr.VolID = partition.PartitionID
	vr.Status = partition.Status
	vr.ReplicaNum = partition.ReplicaNum
	vr.VolType = partition.PartitionType
	vr.Hosts = make([]string, len(partition.PersistenceHosts))
	copy(vr.Hosts, partition.PersistenceHosts)
	return
}

func (partition *DataPartition) checkLoadVolResponse(volTimeOutSec int64) (isResponse bool) {
	partition.Lock()
	defer partition.Unlock()
	for _, addr := range partition.PersistenceHosts {
		volLoc, err := partition.getVolLocation(addr)
		if err != nil {
			return
		}
		loadVolTime := time.Now().Unix() - partition.LastLoadTime
		if volLoc.LoadPartitionIsResponse == false && loadVolTime > LoadVolWaitTime {
			msg := fmt.Sprintf("action[checkLoadVolResponse], volId:%v on Node:%v no response, spent time %v s", partition.PartitionID, addr, loadVolTime)
			log.LogWarn(msg)
			return
		}
		if volLoc.IsLive(volTimeOutSec) == false || volLoc.LoadPartitionIsResponse == false {
			return
		}
	}
	isResponse = true

	return
}

func (partition *DataPartition) getVolLocationByIndex(index uint8) (volLoc *DataReplica) {
	return partition.Locations[int(index)]
}

func (partition *DataPartition) getFileCount() {
	var msg string
	needDelFiles := make([]string, 0)
	partition.Lock()
	defer partition.Unlock()
	for _, volLoc := range partition.Locations {
		volLoc.FileCount = 0
	}
	for _, fc := range partition.FileInCoreMap {
		if fc.MarkDel == true {
			continue
		}
		if len(fc.Metas) == 0 {
			needDelFiles = append(needDelFiles, fc.Name)
		}
		for _, vfNode := range fc.Metas {
			volLoc := partition.getVolLocationByIndex(vfNode.LocIndex)
			volLoc.FileCount++
		}

	}

	for _, vfName := range needDelFiles {
		delete(partition.FileInCoreMap, vfName)
	}

	for _, volLoc := range partition.Locations {
		msg = fmt.Sprintf(GetVolLocationFileCountInfo+"vol:%v  volLocation:%v  FileCount:%v  "+
			"NodeIsActive:%v  VlocIsActive:%v  .VolStatusOnNode:%v ", partition.PartitionID, volLoc.Addr, volLoc.FileCount,
			volLoc.GetVolLocationNode().isActive, volLoc.IsActive(DefaultVolTimeOutSec), volLoc.Status)
		log.LogInfo(msg)
	}

}

func (partition *DataPartition) ReleaseVol() {
	partition.Lock()
	defer partition.Unlock()
	liveLocs := partition.getLiveVolsByPersistenceHosts(DefaultVolTimeOutSec)
	for _, volLoc := range liveLocs {
		volLoc.LoadPartitionIsResponse = false
	}
	for name, fc := range partition.FileInCoreMap {
		fc.Metas = nil
		delete(partition.FileInCoreMap, name)
	}
	partition.FileInCoreMap = make(map[string]*FileInCore, 0)

}

func (partition *DataPartition) IsInVolLocs(host string) (volLoc *DataReplica, ok bool) {
	for _, volLoc = range partition.Locations {
		if volLoc.Addr == host {
			ok = true
			break
		}
	}
	return
}

func (partition *DataPartition) checkReplicaNum(c *Cluster, nsName string) {
	partition.Lock()
	defer partition.Unlock()
	if int(partition.ReplicaNum) != len(partition.PersistenceHosts) {
		orgGoal := partition.ReplicaNum
		partition.ReplicaNum = (uint8)(len(partition.PersistenceHosts))
		partition.UpdateVolHosts(c, nsName)
		msg := fmt.Sprintf("FIX VOL GOAL,vol:%v orgGoal:%v volHOST:%v",
			partition.PartitionID, orgGoal, partition.VolHostsToString())
		log.LogWarn(msg)
	}
}

func (partition *DataPartition) VolHostsToString() (hosts string) {
	return strings.Join(partition.PersistenceHosts, UnderlineSeparator)
}

func (partition *DataPartition) UpdateVolHosts(c *Cluster, nsName string) error {
	return c.syncUpdateVolGroup(nsName, partition)
}

func (partition *DataPartition) setVolToNormal() {
	partition.Lock()
	defer partition.Unlock()
	partition.isRecover = false
}

func (partition *DataPartition) isInPersistenceHosts(volAddr string) (ok bool) {
	for _, addr := range partition.PersistenceHosts {
		if addr == volAddr {
			ok = true
			break
		}
	}

	return
}

func (partition *DataPartition) checkVolReplicationTask() (tasks []*proto.AdminTask) {
	var msg string
	tasks = make([]*proto.AdminTask, 0)
	if excessAddr, excessErr := partition.deleteExcessReplication(); excessErr != nil {
		msg = fmt.Sprintf("action[%v], vol:%v  Excess Replication"+
			" On :%v  Err:%v  rocksDBRecords:%v  so please Delete DataReplica BY SHOUGONG",
			DeleteExcessReplicationErr, partition.PartitionID, excessAddr, excessErr.Error(), partition.PersistenceHosts)
		log.LogWarn(msg)
	}
	if partition.Status == VolReadWrite {
		return
	}
	if lackTask, lackAddr, lackErr := partition.addLackReplication(); lackErr != nil {
		tasks = append(tasks, lackTask)
		msg = fmt.Sprintf("action[%v], vol:%v  Lack Replication"+
			" On :%v  Err:%v  rocksDBRecords:%v  NewTask Create DataReplica",
			AddLackReplicationErr, partition.PartitionID, lackAddr, lackErr.Error(), partition.PersistenceHosts)
		log.LogWarn(msg)
	} else {
		partition.setVolToNormal()
	}

	return
}

/*delete vol excess replication ,range all volLocs
if volLocation not in volRocksDBHosts then generator task to delete volume*/
func (partition *DataPartition) deleteExcessReplication() (excessAddr string, err error) {
	partition.Lock()
	defer partition.Unlock()
	for i := 0; i < len(partition.Locations); i++ {
		volLoc := partition.Locations[i]
		if ok := partition.isInPersistenceHosts(volLoc.Addr); !ok {
			excessAddr = volLoc.Addr
			log.LogError(fmt.Sprintf("action[deleteExcessReplication],volID:%v,has excess replication:%v",
				partition.PartitionID, excessAddr))
			err = DataReplicaExcessError
			break
		}
	}

	return
}

/*add vol lack replication,range all volRocksDBHost if volHosts not in volLocations,
then generator a task to OpRecoverCreateVol to a new Node*/
func (partition *DataPartition) addLackReplication() (t *proto.AdminTask, lackAddr string, err error) {
	partition.Lock()
	for _, addr := range partition.PersistenceHosts {
		if ok := partition.isInPersistenceHosts(addr); !ok {
			log.LogError(fmt.Sprintf("action[addLackReplication],volID:%v lack replication:%v",
				partition.PartitionID, addr))
			err = DataReplicaLackError
			lackAddr = addr

			t = proto.NewAdminTask(proto.OpCreateVol, addr, newCreateVolRequest(partition.PartitionType, partition.PartitionID))
			t.ID = fmt.Sprintf("%v_volID[%v]", t.ID, partition.PartitionID)
			partition.isRecover = true
			break
		}
	}
	partition.Unlock()

	return
}

func (partition *DataPartition) getLiveVols(volTimeOutSec int64) (vols []*DataReplica) {
	vols = make([]*DataReplica, 0)
	for i := 0; i < len(partition.Locations); i++ {
		vol := partition.Locations[i]
		if vol.IsLive(volTimeOutSec) == true && partition.isInPersistenceHosts(vol.Addr) == true {
			vols = append(vols, vol)
		}
	}

	return
}

//live vol that host is in the persistenceHosts, and vol location is alive
func (partition *DataPartition) getLiveVolsByPersistenceHosts(volTimeOutSec int64) (vols []*DataReplica) {
	vols = make([]*DataReplica, 0)
	for _, host := range partition.PersistenceHosts {
		volLoc, ok := partition.IsInVolLocs(host)
		if !ok {
			continue
		}
		if volLoc.IsLive(volTimeOutSec) == true {
			vols = append(vols, volLoc)
		}
	}

	return
}

func (partition *DataPartition) checkAndRemoveMissVol(addr string) {
	if _, ok := partition.MissNodes[addr]; ok {
		delete(partition.MissNodes, addr)
	}
}

func (partition *DataPartition) LoadFile(dataNode *DataNode, resp *proto.LoadVolResponse) {
	partition.Lock()
	defer partition.Unlock()

	index, err := partition.getVolLocationIndex(dataNode.Addr)
	if err != nil {
		msg := fmt.Sprintf("LoadFile volID:%v  on Node:%v  don't report :%v ", partition.PartitionID, dataNode.Addr, err)
		log.LogWarn(msg)
		return
	}
	volLoc := partition.Locations[index]
	volLoc.LoadPartitionIsResponse = true
	for _, vf := range resp.VolSnapshot {
		if vf == nil {
			continue
		}
		fc, ok := partition.FileInCoreMap[vf.Name]
		if !ok {
			fc = NewFileInCore(vf.Name)
			partition.FileInCoreMap[vf.Name] = fc
		}
		fc.updateFileInCore(partition.PartitionID, vf, volLoc, index)
	}
}

func (partition *DataPartition) getVolLocationIndex(addr string) (volLocIndex int, err error) {
	for volLocIndex = 0; volLocIndex < len(partition.Locations); volLocIndex++ {
		volLoc := partition.Locations[volLocIndex]
		if volLoc.Addr == addr {
			return
		}
	}
	log.LogError(fmt.Sprintf("action[getVolLocationIndex],volID:%v,location:%v,err:%v",
		partition.PartitionID, addr, DataReplicaNotFound))
	return -1, DataReplicaNotFound
}

func (partition *DataPartition) DeleteFileOnNode(delAddr, FileID string) {
	partition.Lock()
	defer partition.Unlock()
	fc, ok := partition.FileInCoreMap[FileID]
	if !ok || fc.MarkDel == false {
		return
	}
	volLoc, err := partition.getVolLocation(delAddr)
	if err != nil {
		return
	}
	fc.deleteFileInNode(partition.PartitionID, volLoc)

	msg := fmt.Sprintf("vol:%v  File:%v  on node:%v  delete success",
		partition.PartitionID, fc.Name, delAddr)
	log.LogInfo(msg)

	if len(fc.Metas) == 0 {
		delete(partition.FileInCoreMap, fc.Name)
		msg = fmt.Sprintf("vol:%v  File:%v  delete success on allNode", partition.PartitionID, fc.Name)
		log.LogInfo(msg)
	}

	return
}

func (partition *DataPartition) removeVolHosts(removeAddr string, c *Cluster, nsName string) (err error) {
	orgGoal := len(partition.PersistenceHosts)
	orgVolHosts := make([]string, len(partition.PersistenceHosts))
	copy(orgVolHosts, partition.PersistenceHosts)

	if ok := partition.removeVolHostOnUnderStore(removeAddr); !ok {
		return
	}
	partition.ReplicaNum = (uint8)(len(partition.PersistenceHosts))
	if err = partition.UpdateVolHosts(c, nsName); err != nil {
		partition.ReplicaNum = (uint8)(orgGoal)
		partition.PersistenceHosts = orgVolHosts
	}

	msg := fmt.Sprintf("RemoveVolHostsInfo  vol:%v  Delete host:%v  on PersistenceHosts:%v ",
		partition.PartitionID, removeAddr, partition.PersistenceHosts)
	log.LogDebug(msg)

	return
}

func (partition *DataPartition) removeVolHostOnUnderStore(removeAddr string) (ok bool) {
	for index, addr := range partition.PersistenceHosts {
		if addr == removeAddr {
			after := partition.PersistenceHosts[index+1:]
			partition.PersistenceHosts = partition.PersistenceHosts[:index]
			partition.PersistenceHosts = append(partition.PersistenceHosts, after...)
			ok = true
			break
		}
	}

	return
}

func (partition *DataPartition) addVolHosts(addAddr string, c *Cluster, nsName string) (err error) {
	orgVolHosts := make([]string, len(partition.PersistenceHosts))
	orgGoal := len(partition.PersistenceHosts)
	copy(orgVolHosts, partition.PersistenceHosts)
	for _, addr := range partition.PersistenceHosts {
		if addr == addAddr {
			return
		}
	}
	partition.PersistenceHosts = append(partition.PersistenceHosts, addAddr)
	partition.ReplicaNum = uint8(len(partition.PersistenceHosts))
	if err = partition.UpdateVolHosts(c, nsName); err != nil {
		partition.PersistenceHosts = orgVolHosts
		partition.ReplicaNum = uint8(orgGoal)
		return
	}
	msg := fmt.Sprintf(" AddVolHostsInfo vol:%v  Add host:%v  PersistenceHosts:%v ",
		partition.PartitionID, addAddr, partition.PersistenceHosts)
	log.LogDebug(msg)
	return
}

func (partition *DataPartition) UpdateVol(vr *proto.VolReport, dataNode *DataNode) {
	partition.Lock()
	volLoc, err := partition.getVolLocation(dataNode.Addr)
	partition.Unlock()

	if err != nil && !partition.isInPersistenceHosts(dataNode.Addr) {
		return
	}
	if err != nil && partition.isInPersistenceHosts(dataNode.Addr) {
		volLoc = NewDataReplica(dataNode)
		partition.addMember(volLoc)
	}
	volLoc.Status = (uint8)(vr.VolStatus)
	volLoc.Total = vr.Total
	volLoc.Used = vr.Used
	volLoc.SetAlive()
	partition.Lock()
	partition.checkAndRemoveMissVol(dataNode.Addr)
	partition.Unlock()
}
