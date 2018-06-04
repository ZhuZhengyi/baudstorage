package master

import (
	"fmt"
	"github.com/juju/errors"
	"github.com/tiglabs/baudstorage/proto"
	"github.com/tiglabs/baudstorage/raftstore"
	"github.com/tiglabs/baudstorage/util"
	"github.com/tiglabs/baudstorage/util/log"
	"sync"
	"time"
)

type Cluster struct {
	Name                    string
	namespaces              map[string]*NameSpace
	dataNodes               sync.Map
	metaNodes               sync.Map
	createDataPartitionLock sync.Mutex
	createNsLock            sync.Mutex
	leaderInfo              *LeaderInfo
	cfg                     *ClusterConfig
	fsm                     *MetadataFsm
	partition               raftstore.Partition
	idAlloc                 *IDAllocator
	t                       *Topology
}

func newCluster(name string, leaderInfo *LeaderInfo, fsm *MetadataFsm, partition raftstore.Partition) (c *Cluster) {
	c = new(Cluster)
	c.Name = name
	c.leaderInfo = leaderInfo
	c.namespaces = make(map[string]*NameSpace, 0)
	c.cfg = NewClusterConfig()
	c.fsm = fsm
	c.partition = partition
	c.idAlloc = newIDAllocator(c.fsm.store, c.partition)
	c.t = NewTopology()
	c.startCheckDataPartitions()
	c.startCheckBackendLoadDataPartitions()
	c.startCheckReleaseDataPartitions()
	c.startCheckHeartbeat()
	c.startCheckMetaPartitions()
	return
}

func (c *Cluster) getMasterAddr() (addr string) {
	return c.leaderInfo.addr
}

func (c *Cluster) startCheckDataPartitions() {
	go func() {
		for {
			if c.partition.IsLeader() {
				for _, ns := range c.namespaces {
					c.checkDataPartitions(ns)
				}
			}
			time.Sleep(time.Second * time.Duration(c.cfg.CheckDataPartitionIntervalSeconds))
		}
	}()
}

func (c *Cluster) startCheckBackendLoadDataPartitions() {
	go func() {
		for {
			if c.partition.IsLeader() {
				for _, ns := range c.namespaces {
					c.backendLoadDataPartition(ns)
				}
			}
			time.Sleep(time.Second)
		}
	}()
}

func (c *Cluster) startCheckReleaseDataPartitions() {
	go func() {
		for {
			if c.partition.IsLeader() {
				for _, ns := range c.namespaces {
					c.processReleaseDataPartitionAfterLoad(ns)
				}
			}
			time.Sleep(time.Second * DefaultReleaseDataPartitionInternalSeconds)
		}
	}()
}

func (c *Cluster) startCheckHeartbeat() {
	go func() {
		for {
			if c.partition.IsLeader() {
				c.checkDataNodeHeartbeat()
			}
			time.Sleep(time.Second * DefaultCheckHeartbeatIntervalSeconds)
		}
	}()

	go func() {
		for {
			if c.partition.IsLeader() {
				c.checkMetaNodeHeartbeat()
			}
			time.Sleep(time.Second * DefaultCheckHeartbeatIntervalSeconds)
		}
	}()
}

func (c *Cluster) checkDataNodeHeartbeat() {
	tasks := make([]*proto.AdminTask, 0)
	c.dataNodes.Range(func(addr, dataNode interface{}) bool {
		node := dataNode.(*DataNode)
		node.checkHeartBeat()
		task := node.generateHeartbeatTask(c.getMasterAddr())
		tasks = append(tasks, task)
		return true
	})
	c.putDataNodeTasks(tasks)
}

func (c *Cluster) checkMetaNodeHeartbeat() {
	tasks := make([]*proto.AdminTask, 0)
	c.metaNodes.Range(func(addr, metaNode interface{}) bool {
		node := metaNode.(*MetaNode)
		node.checkHeartbeat()
		task := node.generateHeartbeatTask(c.getMasterAddr())
		tasks = append(tasks, task)
		return true
	})
	c.putMetaNodeTasks(tasks)
}

func (c *Cluster) startCheckMetaPartitions() {
	go func() {
		for {
			if c.partition.IsLeader() {
				for _, ns := range c.namespaces {
					c.checkMetaPartitions(ns)
				}
			}
			time.Sleep(time.Second * time.Duration(c.cfg.CheckDataPartitionIntervalSeconds))
		}
	}()
}

func (c *Cluster) addMetaNode(nodeAddr string) (id uint64, err error) {
	var (
		metaNode *MetaNode
	)
	if value, ok := c.metaNodes.Load(nodeAddr); ok {
		metaNode = value.(*MetaNode)
		return metaNode.ID, nil
	}
	metaNode = NewMetaNode(nodeAddr, c.Name)

	if id, err = c.idAlloc.allocateMetaNodeID(); err != nil {
		goto errDeal
	}
	metaNode.ID = id
	if err = c.syncAddMetaNode(metaNode); err != nil {
		goto errDeal
	}
	c.metaNodes.Store(nodeAddr, metaNode)
	return
errDeal:
	err = fmt.Errorf("action[addMetaNode],metaNodeAddr:%v err:%v ", nodeAddr, err.Error())
	log.LogError(errors.ErrorStack(err))
	Warn(c.Name, err.Error())
	return
}

func (c *Cluster) addDataNode(nodeAddr string) (err error) {
	var dataNode *DataNode
	if _, ok := c.dataNodes.Load(nodeAddr); ok {
		return
	}

	dataNode = NewDataNode(nodeAddr, c.Name)
	if err = c.syncAddDataNode(dataNode); err != nil {
		goto errDeal
	}
	c.dataNodes.Store(nodeAddr, dataNode)
	return
errDeal:
	err = fmt.Errorf("action[addMetaNode],metaNodeAddr:%v err:%v ", nodeAddr, err.Error())
	log.LogError(errors.ErrorStack(err))
	Warn(c.Name, err.Error())
	return err
}

func (c *Cluster) getDataPartitionByID(partitionID uint64) (dp *DataPartition, err error) {
	for _, ns := range c.namespaces {
		if dp, err = ns.getDataPartitionByID(partitionID); err == nil {
			return
		}
	}
	return
}

func (c *Cluster) getMetaPartitionByID(id uint64) (mp *MetaPartition, err error) {
	for _, ns := range c.namespaces {
		if mp, err = ns.getMetaPartition(id); err == nil {
			return
		}
	}
	return
}

func (c *Cluster) getNamespace(nsName string) (ns *NameSpace, err error) {
	ns, ok := c.namespaces[nsName]
	if !ok {
		err = NamespaceNotFound
	}
	return
}

func (c *Cluster) createDataPartition(nsName, partitionType string) (dp *DataPartition, err error) {
	var (
		ns          *NameSpace
		partitionID uint64
		tasks       []*proto.AdminTask
		targetHosts []string
	)
	c.createDataPartitionLock.Lock()
	defer c.createDataPartitionLock.Unlock()
	if ns, err = c.getNamespace(nsName); err != nil {
		goto errDeal
	}
	if targetHosts, err = c.ChooseTargetDataHosts(int(ns.dpReplicaNum)); err != nil {
		goto errDeal
	}
	if partitionID, err = c.idAlloc.allocateDataPartitionID(); err != nil {
		goto errDeal
	}
	dp = newDataPartition(partitionID, ns.dpReplicaNum, partitionType)
	dp.PersistenceHosts = targetHosts
	if err = c.syncAddDataPartition(nsName, dp); err != nil {
		goto errDeal
	}
	tasks = dp.generateCreateTasks()
	c.putDataNodeTasks(tasks)
	ns.dataPartitions.putDataPartition(dp)

	return
errDeal:
	err = fmt.Errorf("action[createDataPartition], Err:%v ", err.Error())
	log.LogError(errors.ErrorStack(err))
	Warn(c.Name, err.Error())
	return
}

func (c *Cluster) ChooseTargetDataHosts(replicaNum int) (hosts []string, err error) {
	var (
		masterAddr []string
		addrs      []string
		racks      []*Rack
		rack       *Rack
	)
	hosts = make([]string, 0)
	if c.isSingleRack() {
		var newHosts []string
		if rack, err = c.t.getRack(c.t.racks[0]); err != nil {
			return nil, errors.Trace(err)
		}
		if newHosts, err = rack.getAvailDataNodeHosts(hosts, replicaNum); err != nil {
			return nil, errors.Trace(err)
		}
		hosts = newHosts
		return
	}

	if racks, err = c.t.allocRacks(replicaNum, nil); err != nil {
		return
	}

	if len(racks) == 2 {
		masterRack := racks[0]
		slaveRack := racks[1]
		masterReplicaNum := replicaNum/2 + 1
		slaveReplicaNum := replicaNum - masterReplicaNum
		if masterAddr, err = masterRack.getAvailDataNodeHosts(hosts, masterReplicaNum); err != nil {
			return nil, errors.Trace(err)
		}
		hosts = append(hosts, masterAddr...)
		if addrs, err = slaveRack.getAvailDataNodeHosts(hosts, slaveReplicaNum); err != nil {
			return nil, errors.Trace(err)
		}
		hosts = append(hosts, addrs...)
	} else if len(racks) == replicaNum {
		for index := 0; index < replicaNum; index++ {
			rack := racks[index]
			if addrs, err = rack.getAvailDataNodeHosts(hosts, 1); err != nil {
				return nil, errors.Trace(err)
			}
			hosts = append(hosts, addrs...)
		}
	}
	if len(hosts) != replicaNum {
		return nil, NoAnyDataNodeForCreateDataPartition
	}
	return
}

func (c *Cluster) getDataNode(addr string) (dataNode *DataNode, err error) {
	value, ok := c.dataNodes.Load(addr)
	if !ok {
		err = DataNodeNotFound
		return
	}
	dataNode = value.(*DataNode)
	return
}

func (c *Cluster) getMetaNode(addr string) (metaNode *MetaNode, err error) {
	value, ok := c.metaNodes.Load(addr)
	if !ok {
		err = MetaNodeNotFound
		return
	}
	metaNode = value.(*MetaNode)
	return
}

func (c *Cluster) dataNodeOffLine(dataNode *DataNode) {
	msg := fmt.Sprintf("action[dataNodeOffLine], Node[%v] OffLine", dataNode.Addr)
	log.LogWarn(msg)
	for _, ns := range c.namespaces {
		for _, vg := range ns.dataPartitions.dataPartitions {
			c.dataPartitionOffline(dataNode.Addr, ns.Name, vg, DataNodeOfflineInfo)
		}
	}
	c.dataNodes.Delete(dataNode.Addr)

}

func (c *Cluster) dataPartitionOffline(offlineAddr, nsName string, dp *DataPartition, errMsg string) {
	var (
		newHosts []string
		newAddr  string
		msg      string
		tasks    []*proto.AdminTask
		task     *proto.AdminTask
		err      error
		dataNode *DataNode
		rack     *Rack
		ns       *NameSpace
	)
	dp.Lock()
	defer dp.Unlock()
	if ok := dp.isInPersistenceHosts(offlineAddr); !ok {
		return
	}

	if ns, err = c.getNamespace(nsName); err != nil {
		goto errDeal
	}

	if err = dp.hasMissOne(int(ns.dpReplicaNum)); err != nil {
		goto errDeal
	}
	if err = dp.canOffLine(offlineAddr); err != nil {
		goto errDeal
	}
	dp.generatorOffLineLog(offlineAddr)

	if dataNode, err = c.getDataNode(dp.PersistenceHosts[0]); err != nil {
		goto errDeal
	}
	if rack, err = c.t.getRack(dataNode.RackName); err != nil {
		goto errDeal
	}
	if newHosts, err = rack.getAvailDataNodeHosts(dp.PersistenceHosts, 1); err != nil {
		goto errDeal
	}
	if err = dp.removeHosts(offlineAddr, c, nsName); err != nil {
		goto errDeal
	}
	newAddr = newHosts[0]
	if err = dp.addHosts(newAddr, c, nsName); err != nil {
		goto errDeal
	}
	dp.offLineInMem(offlineAddr)
	dp.checkAndRemoveMissReplica(offlineAddr)
	task = proto.NewAdminTask(proto.OpCreateDataPartition, offlineAddr, newCreateVolRequest(dp.PartitionType, dp.PartitionID))
	task.ID = fmt.Sprintf("%v_volID[%v]", task.ID, dp.PartitionID)
	tasks = make([]*proto.AdminTask, 0)
	tasks = append(tasks, task)
	c.putDataNodeTasks(tasks)
	goto errDeal
errDeal:
	msg = fmt.Sprintf(errMsg+" vol:%v  on Node:%v  "+
		"DiskError  TimeOut Report Then Fix It on newHost:%v   Err:%v , PersistenceHosts:%v  ",
		dp.PartitionID, offlineAddr, newAddr, err, dp.PersistenceHosts)
	if err != nil {
		Warn(c.Name, msg)
	}
	log.LogWarn(msg)
}

func (c *Cluster) metaNodeOffLine(metaNode *MetaNode) {
	msg := fmt.Sprintf("action[metaNodeOffLine], Node[%v] OffLine", metaNode.Addr)
	log.LogWarn(msg)
	for _, ns := range c.namespaces {
		for _, mp := range ns.MetaPartitions {
			c.metaPartitionOffline(ns.Name, metaNode.Addr, mp.PartitionID)
		}
	}
	c.metaNodes.Delete(metaNode.Addr)
}

func (c *Cluster) createNamespace(name string, replicaNum uint8) (err error) {
	var (
		ns *NameSpace
	)
	c.createNsLock.Lock()
	defer c.createNsLock.Unlock()
	if _, ok := c.namespaces[name]; ok {
		err = hasExist(name)
		goto errDeal
	}
	ns = NewNameSpace(name, replicaNum)
	if err = c.syncAddNamespace(ns); err != nil {
		goto errDeal
	}
	c.namespaces[name] = ns
	if err = c.CreateMetaPartition(name, 0, DefaultMaxMetaPartitionInodeID); err != nil {
		delete(c.namespaces, name)
		goto errDeal
	}
	return
errDeal:
	err = fmt.Errorf("action[createNamespace], name:%v, err:%v ", name, err.Error())
	log.LogError(errors.ErrorStack(err))
	Warn(c.Name, err.Error())
	return
}

func (c *Cluster) CreateMetaPartition(nsName string, start, end uint64) (err error) {
	var (
		ns          *NameSpace
		mp          *MetaPartition
		hosts       []string
		partitionID uint64
		peers       []proto.Peer
	)
	ns, ok := c.namespaces[nsName]
	if !ok {
		err = elementNotFound(nsName)
		return
	}

	if hosts, peers, err = c.ChooseTargetMetaHosts(int(ns.mpReplicaNum)); err != nil {
		return errors.Trace(err)
	}
	log.LogDebugf("target meta hosts:%v,peers:%v", hosts, peers)
	if partitionID, err = c.idAlloc.allocateMetaPartitionID(); err != nil {
		return errors.Trace(err)
	}
	mp = NewMetaPartition(partitionID, start, end, nsName)
	mp.setPersistenceHosts(hosts)
	mp.setPeers(peers)
	if err = c.syncAddMetaPartition(nsName, mp); err != nil {
		return errors.Trace(err)
	}
	ns.AddMetaPartition(mp)
	c.putMetaNodeTasks(mp.generateCreateMetaPartitionTasks(nil, nsName))
	return
}

func (c *Cluster) ChooseTargetMetaHosts(replicaNum int) (hosts []string, peers []proto.Peer, err error) {
	var (
		masterAddr []string
		slaveAddrs []string
		masterPeer []proto.Peer
		slavePeers []proto.Peer
	)
	hosts = make([]string, 0)
	if masterAddr, masterPeer, err = c.getAvailMetaNodeHosts("", hosts, 1); err != nil {
		return nil, nil, errors.Trace(err)
	}
	peers = append(peers, masterPeer...)
	hosts = append(hosts, masterAddr[0])
	otherReplica := replicaNum - 1
	if otherReplica == 0 {
		return
	}
	metaNode, err := c.getMetaNode(masterAddr[0])
	if err != nil {
		return nil, nil, errors.Trace(err)
	}
	if slaveAddrs, slavePeers, err = c.getAvailMetaNodeHosts(metaNode.RackName, hosts, otherReplica); err != nil {
		return nil, nil, errors.Trace(err)
	}
	hosts = append(hosts, slaveAddrs...)
	peers = append(peers, slavePeers...)
	if len(hosts) != replicaNum {
		return nil, nil, NoAnyMetaNodeForCreateMetaPartition
	}
	return
}

func (c *Cluster) DataNodeCount() (len int) {

	c.dataNodes.Range(func(key, value interface{}) bool {
		len++
		return true
	})
	return
}

func (c *Cluster) getAllDataNodes() (dataNodes []DataNodeView) {
	dataNodes = make([]DataNodeView, 0)
	c.dataNodes.Range(func(addr, node interface{}) bool {
		dataNode := node.(*DataNode)
		dataNodes = append(dataNodes, DataNodeView{Addr: dataNode.Addr, Status: dataNode.isActive})
		return true
	})
	return
}

func (c *Cluster) getAllMetaNodes() (metaNodes []MetaNodeView) {
	metaNodes = make([]MetaNodeView, 0)
	c.metaNodes.Range(func(addr, node interface{}) bool {
		metaNode := node.(*MetaNode)
		metaNodes = append(metaNodes, MetaNodeView{ID: metaNode.ID, Addr: metaNode.Addr, Status: metaNode.IsActive})
		return true
	})
	return
}

func (c *Cluster) getAllNamespaces() (namespaces []string) {
	namespaces = make([]string, 0)
	for name := range c.namespaces {
		namespaces = append(namespaces, name)
	}
	return
}

func (c *Cluster) getDataPartitionCapacity(ns *NameSpace) (count int) {
	var (
		totalCount uint64
		mutex      sync.Mutex
	)
	mutex.Lock()
	defer mutex.Unlock()
	c.dataNodes.Range(func(addr, value interface{}) bool {
		dataNode := value.(*DataNode)
		totalCount = totalCount + dataNode.RemainWeightsForCreateVol/util.DefaultVolSize
		return true
	})
	count = int(totalCount / uint64(ns.dpReplicaNum))
	return
}
