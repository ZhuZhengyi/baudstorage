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
	Replicas         []*DataReplica
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
	partition.Replicas = make([]*DataReplica, 0)
	partition.FileInCoreMap = make(map[string]*FileInCore, 0)
	partition.MissNodes = make(map[string]int64)
	return
}

func (partition *DataPartition) addMember(replica *DataReplica) {
	partition.Lock()
	defer partition.Unlock()
	for _, vol := range partition.Replicas {
		if replica.Addr == vol.Addr {
			return
		}
	}
	partition.Replicas = append(partition.Replicas, replica)
}

func (partition *DataPartition) generateCreateTasks() (tasks []*proto.AdminTask) {
	tasks = make([]*proto.AdminTask, 0)
	for _, addr := range partition.PersistenceHosts {
		t := proto.NewAdminTask(proto.OpCreateDataPartition, addr, newCreateVolRequest(partition.PartitionType, partition.PartitionID))
		partition.resetTaskID(t)
		tasks = append(tasks, t)
	}
	return
}

func (partition *DataPartition) resetTaskID(t *proto.AdminTask) {
	t.ID = fmt.Sprintf("%v_DataPartitionID[%v]", t.ID, partition.PartitionID)
}

func (partition *DataPartition) hasMissOne(replicaNum int) (err error) {
	availPersistenceHostLen := len(partition.PersistenceHosts)
	if availPersistenceHostLen <= replicaNum-1 {
		log.LogError(fmt.Sprintf("action[%v],partitionID:%v,err:%v",
			"hasMissOne", partition.PartitionID, DataReplicaHasMissOneError))
		err = DataReplicaHasMissOneError
	}
	return
}

func (partition *DataPartition) canOffLine(offlineAddr string) (err error) {
	msg := fmt.Sprintf("action[canOffLine],partitionID:%v  RocksDBHost:%v  offLine:%v ",
		partition.PartitionID, partition.PersistenceHosts, offlineAddr)
	liveReplicas := partition.getLiveReplicas(DefaultDataPartitionTimeOutSec)
	if len(liveReplicas) < 2 {
		msg = fmt.Sprintf(msg+" err:%v  liveReplicas:%v ", CannotOffLineErr, len(liveReplicas))
		log.LogError(msg)
		err = fmt.Errorf(msg)
	}

	return
}

func (partition *DataPartition) generatorOffLineLog(offlineAddr string) (msg string) {
	msg = fmt.Sprintf("action[generatorOffLineLog],data partition:%v  offlineaddr:%v  ",
		partition.PartitionID, offlineAddr)
	replicas := partition.GetAvailableDataReplicas()
	for i := 0; i < len(replicas); i++ {
		replica := replicas[i]
		msg += fmt.Sprintf(" addr:%v  dataReplicaStatus:%v  FileCount :%v ", replica.Addr,
			replica.Status, replica.FileCount)
	}
	log.LogWarn(msg)

	return
}

/*获取该副本目前有效的node,即Node在汇报心跳正常，并且该Node不是unavailable*/
func (partition *DataPartition) GetAvailableDataReplicas() (replicas []*DataReplica) {
	replicas = make([]*DataReplica, 0)
	for i := 0; i < len(partition.Replicas); i++ {
		replica := partition.Replicas[i]
		if replica.CheckLocIsAvailContainsDiskError() == true && partition.isInPersistenceHosts(replica.Addr) == true {
			replicas = append(replicas, replica)
		}
	}

	return
}

func (partition *DataPartition) offLineInMem(addr string) {
	delIndex := -1
	var replica *DataReplica
	for i := 0; i < len(partition.Replicas); i++ {
		replica = partition.Replicas[i]
		if replica.Addr == addr {
			delIndex = i
			break
		}
	}
	msg := fmt.Sprintf("action[offLineInMem],data partition:%v  on Node:%v  OffLine,the node is in replicas:%v", partition.PartitionID, addr, replica != nil)
	log.LogDebug(msg)
	if replica == nil {
		return
	}

	for _, fc := range partition.FileInCoreMap {
		fc.deleteFileInNode(partition.PartitionID, replica)
	}
	partition.DeleteReplicaByIndex(delIndex)

	return
}

func (partition *DataPartition) DeleteReplicaByIndex(index int) {
	var replicaAddrs []string
	for _, replica := range partition.Replicas {
		replicaAddrs = append(replicaAddrs, replica.Addr)
	}
	msg := fmt.Sprintf("DeleteReplicaByIndex replica:%v  index:%v  locations :%v ", partition.PartitionID, index, replicaAddrs)
	log.LogInfo(msg)
	replicasAfter := partition.Replicas[index+1:]
	partition.Replicas = partition.Replicas[:index]
	partition.Replicas = append(partition.Replicas, replicasAfter...)
}

func (partition *DataPartition) generateLoadTasks() (tasks []*proto.AdminTask) {

	partition.Lock()
	defer partition.Unlock()
	for _, addr := range partition.PersistenceHosts {
		replica, err := partition.getReplica(addr)
		if err != nil || replica.IsLive(DefaultDataPartitionTimeOutSec) == false {
			continue
		}
		replica.LoadPartitionIsResponse = false
		t := proto.NewAdminTask(proto.OpLoadDataPartition, replica.Addr, newLoadVolMetricRequest(partition.PartitionType, partition.PartitionID))
		partition.resetTaskID(t)
		tasks = append(tasks, t)
	}
	partition.LastLoadTime = time.Now().Unix()
	return
}

func (partition *DataPartition) getReplica(addr string) (replica *DataReplica, err error) {
	for index := 0; index < len(partition.Replicas); index++ {
		replica = partition.Replicas[index]
		if replica.Addr == addr {
			return
		}
	}
	log.LogError(fmt.Sprintf("action[getReplica],partitionID:%v,locations:%v,err:%v",
		partition.PartitionID, addr, DataReplicaNotFound))
	return nil, DataReplicaNotFound
}

func (partition *DataPartition) convertToDataPartitionResponse() (dpr *DataPartitionResponse) {
	dpr = new(DataPartitionResponse)
	partition.Lock()
	defer partition.Unlock()
	dpr.PartitionID = partition.PartitionID
	dpr.Status = partition.Status
	dpr.ReplicaNum = partition.ReplicaNum
	dpr.PartitionType = partition.PartitionType
	dpr.Hosts = make([]string, len(partition.PersistenceHosts))
	copy(dpr.Hosts, partition.PersistenceHosts)
	return
}

func (partition *DataPartition) checkLoadResponse(volTimeOutSec int64) (isResponse bool) {
	partition.Lock()
	defer partition.Unlock()
	for _, addr := range partition.PersistenceHosts {
		replica, err := partition.getReplica(addr)
		if err != nil {
			return
		}
		loadVolTime := time.Now().Unix() - partition.LastLoadTime
		if replica.LoadPartitionIsResponse == false && loadVolTime > LoadDataPartitionWaitTime {
			msg := fmt.Sprintf("action[checkLoadResponse], partitionID:%v on Node:%v no response, spent time %v s",
				partition.PartitionID, addr, loadVolTime)
			log.LogWarn(msg)
			return
		}
		if replica.IsLive(volTimeOutSec) == false || replica.LoadPartitionIsResponse == false {
			return
		}
	}
	isResponse = true

	return
}

func (partition *DataPartition) getReplicaByIndex(index uint8) (replica *DataReplica) {
	return partition.Replicas[int(index)]
}

func (partition *DataPartition) getFileCount() {
	var msg string
	needDelFiles := make([]string, 0)
	partition.Lock()
	defer partition.Unlock()
	for _, replica := range partition.Replicas {
		replica.FileCount = 0
	}
	for _, fc := range partition.FileInCoreMap {
		if fc.MarkDel == true {
			continue
		}
		if len(fc.Metas) == 0 {
			needDelFiles = append(needDelFiles, fc.Name)
		}
		for _, vfNode := range fc.Metas {
			replica := partition.getReplicaByIndex(vfNode.LocIndex)
			replica.FileCount++
		}

	}

	for _, vfName := range needDelFiles {
		delete(partition.FileInCoreMap, vfName)
	}

	for _, replica := range partition.Replicas {
		msg = fmt.Sprintf(GetDataReplicaFileCountInfo+"partitionID:%v  replicaAddr:%v  FileCount:%v  "+
			"NodeIsActive:%v  replicaIsActive:%v  .replicaStatusOnNode:%v ", partition.PartitionID, replica.Addr, replica.FileCount,
			replica.GetVolLocationNode().isActive, replica.IsActive(DefaultDataPartitionTimeOutSec), replica.Status)
		log.LogInfo(msg)
	}

}

func (partition *DataPartition) ReleaseDataPartition() {
	partition.Lock()
	defer partition.Unlock()
	liveReplicas := partition.getLiveReplicasByPersistenceHosts(DefaultDataPartitionTimeOutSec)
	for _, replica := range liveReplicas {
		replica.LoadPartitionIsResponse = false
	}
	for name, fc := range partition.FileInCoreMap {
		fc.Metas = nil
		delete(partition.FileInCoreMap, name)
	}
	partition.FileInCoreMap = make(map[string]*FileInCore, 0)

}

func (partition *DataPartition) IsInReplicas(host string) (replica *DataReplica, ok bool) {
	for _, replica = range partition.Replicas {
		if replica.Addr == host {
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
		partition.UpdateHosts(c, nsName)
		msg := fmt.Sprintf("FIX DataPartition GOAL,partitionID:%v orgGoal:%v curHOST:%v",
			partition.PartitionID, orgGoal, partition.HostsToString())
		log.LogWarn(msg)
	}
}

func (partition *DataPartition) HostsToString() (hosts string) {
	return strings.Join(partition.PersistenceHosts, UnderlineSeparator)
}

func (partition *DataPartition) UpdateHosts(c *Cluster, nsName string) error {
	return c.syncUpdateDataPartition(nsName, partition)
}

func (partition *DataPartition) setToNormal() {
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

func (partition *DataPartition) checkReplicationTask() (tasks []*proto.AdminTask) {
	var msg string
	tasks = make([]*proto.AdminTask, 0)
	if excessAddr, task, excessErr := partition.deleteExcessReplication(); excessErr != nil {
		tasks = append(tasks, task)
		msg = fmt.Sprintf("action[%v], partitionID:%v  Excess Replication"+
			" On :%v  Err:%v  rocksDBRecords:%v",
			DeleteExcessReplicationErr, partition.PartitionID, excessAddr, excessErr.Error(), partition.PersistenceHosts)
		log.LogWarn(msg)

	}
	if partition.Status == DataPartitionReadWrite {
		return
	}
	if lackTask, lackAddr, lackErr := partition.addLackReplication(); lackErr != nil {
		tasks = append(tasks, lackTask)
		msg = fmt.Sprintf("action[%v], partitionID:%v  Lack Replication"+
			" On :%v  Err:%v  rocksDBRecords:%v  NewTask Create DataReplica",
			AddLackReplicationErr, partition.PartitionID, lackAddr, lackErr.Error(), partition.PersistenceHosts)
		log.LogWarn(msg)
	} else {
		partition.setToNormal()
	}

	return
}

/*delete vol excess replication ,range all volLocs
if volLocation not in volRocksDBHosts then generator task to delete volume*/
func (partition *DataPartition) deleteExcessReplication() (excessAddr string, task *proto.AdminTask, err error) {
	partition.Lock()
	defer partition.Unlock()
	for i := 0; i < len(partition.Replicas); i++ {
		replica := partition.Replicas[i]
		if ok := partition.isInPersistenceHosts(replica.Addr); !ok {
			excessAddr = replica.Addr
			log.LogError(fmt.Sprintf("action[deleteExcessReplication],partitionID:%v,has excess replication:%v",
				partition.PartitionID, excessAddr))
			err = DataReplicaExcessError
			task = proto.NewAdminTask(proto.OpDeleteDataPartition, excessAddr, newDeleteDataPartitionRequest(partition.PartitionID))
			break
		}
	}

	return
}

/*add data partition lack replication,range all RocksDBHost if Hosts not in Replicas,
then generator a task to OpRecoverCreateDataPartition to a new Node*/
func (partition *DataPartition) addLackReplication() (t *proto.AdminTask, lackAddr string, err error) {
	partition.Lock()
	for _, addr := range partition.PersistenceHosts {
		if ok := partition.isInPersistenceHosts(addr); !ok {
			log.LogError(fmt.Sprintf("action[addLackReplication],partitionID:%v lack replication:%v",
				partition.PartitionID, addr))
			err = DataReplicaLackError
			lackAddr = addr

			t = proto.NewAdminTask(proto.OpCreateDataPartition, addr, newCreateVolRequest(partition.PartitionType, partition.PartitionID))
			t.ID = fmt.Sprintf("%v_partitionID[%v]", t.ID, partition.PartitionID)
			partition.isRecover = true
			break
		}
	}
	partition.Unlock()

	return
}

func (partition *DataPartition) getLiveReplicas(timeOutSec int64) (replicas []*DataReplica) {
	replicas = make([]*DataReplica, 0)
	for i := 0; i < len(partition.Replicas); i++ {
		replica := partition.Replicas[i]
		if replica.IsLive(timeOutSec) == true && partition.isInPersistenceHosts(replica.Addr) == true {
			replicas = append(replicas, replica)
		}
	}

	return
}

//live replica that host is in the persistenceHosts, and replica location is alive
func (partition *DataPartition) getLiveReplicasByPersistenceHosts(timeOutSec int64) (replicas []*DataReplica) {
	replicas = make([]*DataReplica, 0)
	for _, host := range partition.PersistenceHosts {
		replica, ok := partition.IsInReplicas(host)
		if !ok {
			continue
		}
		if replica.IsLive(timeOutSec) == true {
			replicas = append(replicas, replica)
		}
	}

	return
}

func (partition *DataPartition) checkAndRemoveMissReplica(addr string) {
	if _, ok := partition.MissNodes[addr]; ok {
		delete(partition.MissNodes, addr)
	}
}

func (partition *DataPartition) LoadFile(dataNode *DataNode, resp *proto.LoadDataPartitionResponse) {
	partition.Lock()
	defer partition.Unlock()

	index, err := partition.getReplicaIndex(dataNode.Addr)
	if err != nil {
		msg := fmt.Sprintf("LoadFile volID:%v  on Node:%v  don't report :%v ", partition.PartitionID, dataNode.Addr, err)
		log.LogWarn(msg)
		return
	}
	replica := partition.Replicas[index]
	replica.LoadPartitionIsResponse = true
	for _, dpf := range resp.PartitionSnapshot {
		if dpf == nil {
			continue
		}
		fc, ok := partition.FileInCoreMap[dpf.Name]
		if !ok {
			fc = NewFileInCore(dpf.Name)
			partition.FileInCoreMap[dpf.Name] = fc
		}
		fc.updateFileInCore(partition.PartitionID, dpf, replica, index)
	}
}

func (partition *DataPartition) getReplicaIndex(addr string) (index int, err error) {
	for index = 0; index < len(partition.Replicas); index++ {
		replica := partition.Replicas[index]
		if replica.Addr == addr {
			return
		}
	}
	log.LogError(fmt.Sprintf("action[getReplicaIndex],partitionID:%v,location:%v,err:%v",
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
	replica, err := partition.getReplica(delAddr)
	if err != nil {
		return
	}
	fc.deleteFileInNode(partition.PartitionID, replica)

	msg := fmt.Sprintf("partitionID:%v  File:%v  on node:%v  delete success",
		partition.PartitionID, fc.Name, delAddr)
	log.LogInfo(msg)

	if len(fc.Metas) == 0 {
		delete(partition.FileInCoreMap, fc.Name)
		msg = fmt.Sprintf("partitionID:%v  File:%v  delete success on allNode", partition.PartitionID, fc.Name)
		log.LogInfo(msg)
	}

	return
}

func (partition *DataPartition) removeHosts(removeAddr string, c *Cluster, nsName string) (err error) {
	orgGoal := len(partition.PersistenceHosts)
	orgHosts := make([]string, len(partition.PersistenceHosts))
	copy(orgHosts, partition.PersistenceHosts)

	if ok := partition.removeHostOnUnderStore(removeAddr); !ok {
		return
	}
	partition.ReplicaNum = (uint8)(len(partition.PersistenceHosts))
	if err = partition.UpdateHosts(c, nsName); err != nil {
		partition.ReplicaNum = (uint8)(orgGoal)
		partition.PersistenceHosts = orgHosts
	}

	msg := fmt.Sprintf("action[removeHosts]  partitionID:%v  Delete host:%v  on PersistenceHosts:%v ",
		partition.PartitionID, removeAddr, partition.PersistenceHosts)
	log.LogDebug(msg)

	return
}

func (partition *DataPartition) removeHostOnUnderStore(removeAddr string) (ok bool) {
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

func (partition *DataPartition) addHosts(addAddr string, c *Cluster, nsName string) (err error) {
	orgHosts := make([]string, len(partition.PersistenceHosts))
	orgGoal := len(partition.PersistenceHosts)
	copy(orgHosts, partition.PersistenceHosts)
	for _, addr := range partition.PersistenceHosts {
		if addr == addAddr {
			return
		}
	}
	partition.PersistenceHosts = append(partition.PersistenceHosts, addAddr)
	partition.ReplicaNum = uint8(len(partition.PersistenceHosts))
	if err = partition.UpdateHosts(c, nsName); err != nil {
		partition.PersistenceHosts = orgHosts
		partition.ReplicaNum = uint8(orgGoal)
		return
	}
	msg := fmt.Sprintf(" action[addHosts] partitionID:%v  Add host:%v  PersistenceHosts:%v ",
		partition.PartitionID, addAddr, partition.PersistenceHosts)
	log.LogDebug(msg)
	return
}

func (partition *DataPartition) UpdateDataPartitionMetric(vr *proto.PartitionReport, dataNode *DataNode) {
	partition.Lock()
	replica, err := partition.getReplica(dataNode.Addr)
	partition.Unlock()

	if err != nil && !partition.isInPersistenceHosts(dataNode.Addr) {
		return
	}
	if err != nil && partition.isInPersistenceHosts(dataNode.Addr) {
		replica = NewDataReplica(dataNode)
		partition.addMember(replica)
	}
	replica.Status = (uint8)(vr.PartitionStatus)
	replica.Total = vr.Total
	replica.Used = vr.Used
	replica.SetAlive()
	partition.Lock()
	partition.checkAndRemoveMissReplica(dataNode.Addr)
	partition.Unlock()
}
