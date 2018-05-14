package master

import (
	"fmt"
	"github.com/tiglabs/baudstorage/proto"
	"github.com/tiglabs/baudstorage/util/log"
	"sync"
	"time"
)

type Cluster struct {
	Name          string
	namespaces    map[string]*NameSpace
	dataNodes     sync.Map
	metaNodes     sync.Map
	createVolLock sync.Mutex
	createNsLock  sync.Mutex
	cfg           *ClusterConfig
}

func NewCluster(name string) (c *Cluster) {
	c = new(Cluster)
	c.Name = name
	c.cfg = NewClusterConfig()
	c.startCheckVolGroups()
	c.startCheckBackendLoadVolGroups()
	c.startCheckReleaseVolGroups()
	c.startCheckHearBeat()
	c.startCheckMetaGroups()
	return
}

func (c *Cluster) startCheckVolGroups() {
	go func() {
		for {
			for _, ns := range c.namespaces {
				c.checkVolGroups(ns)
			}
			time.Sleep(time.Second * time.Duration(c.cfg.CheckVolIntervalSeconds))
		}
	}()
}

func (c *Cluster) startCheckBackendLoadVolGroups() {
	go func() {
		for {

			for _, ns := range c.namespaces {
				c.backendLoadVolGroup(ns)
			}
			time.Sleep(time.Second)
		}
	}()
}

func (c *Cluster) startCheckReleaseVolGroups() {
	go func() {
		for {
			for _, ns := range c.namespaces {
				c.processReleaseVolAfterLoadVolGroup(ns)
			}
			time.Sleep(time.Second * DefaultReleaseVolInternalSeconds)
		}
	}()
}

func (c *Cluster) startCheckHearBeat() {
	go func() {
		for {
			tasks := make([]*proto.AdminTask, 0)
			c.dataNodes.Range(func(addr, dataNode interface{}) bool {
				node := dataNode.(*DataNode)
				task := node.generateHeartbeatTask()
				tasks = append(tasks, task)
				return true
			})
			c.putDataNodeTasks(tasks)
			time.Sleep(time.Second * DefaultCheckHeartBeatIntervalSeconds)
		}
	}()

	go func() {
		for {
			tasks := make([]*proto.AdminTask, 0)
			c.metaNodes.Range(func(addr, metaNode interface{}) bool {
				node := metaNode.(*MetaNode)
				task := node.generateHeartbeatTask()
				tasks = append(tasks, task)
				return true
			})
			c.putMetaNodeTasks(tasks)
			time.Sleep(time.Second * DefaultCheckHeartBeatIntervalSeconds)
		}
	}()
}

func (c *Cluster) startCheckMetaGroups() {
	go func() {
		for {
			for _, ns := range c.namespaces {
				c.checkMetaGroups(ns)
			}
			time.Sleep(time.Second * time.Duration(c.cfg.CheckVolIntervalSeconds))
		}
	}()
}

func (c *Cluster) addMetaNode(nodeAddr string) (id uint64, err error) {
	var (
		metaNode *MetaNode
	)
	if _, ok := c.metaNodes.Load(nodeAddr); ok {
		err = hasExist(nodeAddr)
		goto errDeal
	}
	metaNode = NewMetaNode(nodeAddr)

	if id, err = c.getMaxID(); err != nil {
		goto errDeal
	}
	metaNode.id = id
	//todo sync node by raft
	c.metaNodes.Store(nodeAddr, metaNode)
	return
errDeal:
	err = fmt.Errorf("action[addMetaNode],metaNodeAddr:%v err:%v ", nodeAddr, err.Error())
	log.LogWarn(err.Error())
	return
}

func (c *Cluster) addDataNode(nodeAddr string) (err error) {
	var dataNode *DataNode
	if _, ok := c.dataNodes.Load(nodeAddr); ok {
		err = hasExist(nodeAddr)
		goto errDeal
	}

	dataNode = NewDataNode(nodeAddr)
	//todo sync node by raft

	c.dataNodes.Store(nodeAddr, dataNode)
	return
errDeal:
	err = fmt.Errorf("action[addMetaNode],metaNodeAddr:%v err:%v ", nodeAddr, err.Error())
	log.LogWarn(err.Error())
	return err
}

func (c *Cluster) getVolsView() (body []byte, err error) {
	body = make([]byte, 0)
	for _, ns := range c.namespaces {
		if partBody, err := ns.volGroups.updateVolResponseCache(NoNeedUpdateVolResponse, 0); err == nil {
			body = append(body, partBody...)
		} else {
			log.LogError(fmt.Sprintf("getVolsView on namespace %v err:%v", ns.Name, err.Error()))
		}

	}
	return
}

func (c *Cluster) getVolGroupByVolID(volID uint64) (vol *VolGroup, err error) {
	for _, ns := range c.namespaces {
		if vol, err = ns.getVolGroupByVolID(volID); err == nil {
			return
		}
	}
	return
}

func (c *Cluster) getMetaPartitionByID(id uint64) (mp *MetaPartition, err error) {
	for _, ns := range c.namespaces {
		if mp, err = ns.getMetaPartitionById(id); err == nil {
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

func (c *Cluster) createVolGroup(nsName string) (vg *VolGroup, err error) {
	var (
		ns          *NameSpace
		volID       uint64
		tasks       []*proto.AdminTask
		targetHosts []string
	)
	c.createVolLock.Lock()
	defer c.createVolLock.Unlock()
	if ns, err = c.getNamespace(nsName); err != nil {
		goto errDeal
	}

	if volID, err = c.getMaxID(); err != nil {
		goto errDeal
	}
	//volID++
	vg = newVolGroup(volID, ns.volReplicaNum)
	if targetHosts, err = c.ChooseTargetDataHosts(int(ns.volReplicaNum)); err != nil {
		goto errDeal
	}
	vg.PersistenceHosts = targetHosts
	//todo sync and persistence hosts to other node in the cluster
	tasks = vg.generateCreateVolGroupTasks()
	c.putDataNodeTasks(tasks)
	ns.volGroups.putVol(vg)

	return
errDeal:
	err = fmt.Errorf("action[createVolGroup], Err:%v ", err.Error())
	log.LogError(err.Error())
	return
}

func (c *Cluster) ChooseTargetDataHosts(replicaNum int) (hosts []string, err error) {
	var (
		masterAddr []string
		slaveAddrs []string
	)
	hosts = make([]string, 0)
	if masterAddr, err = c.getAvailDataNodeHosts("", hosts, 1); err != nil {
		return
	}
	hosts = append(hosts, masterAddr[0])
	otherReplica := replicaNum - 1
	if otherReplica == 0 {
		return
	}
	dataNode, err := c.getDataNode(masterAddr[0])
	if err != nil {
		return
	}
	if slaveAddrs, err = c.getAvailDataNodeHosts(dataNode.RackName, hosts, otherReplica); err != nil {
		return
	}
	hosts = append(hosts, slaveAddrs...)
	if len(hosts) != replicaNum {
		return nil, NoAnyDataNodeForCreateVol
	}
	return
}

func (c *Cluster) getMaxID() (id uint64, err error) {
	//todo getMaxID from raft
	if err != nil {
		goto errDeal
	}
	return
errDeal:
	err = fmt.Errorf("action[getMaxID], Err:%v ", err.Error())
	log.LogError(err.Error())
	return
}

func (c *Cluster) getDataNode(addr string) (dataNode *DataNode, err error) {
	value, ok := c.dataNodes.Load(addr)
	if !ok {
		err = DataNodeNotFound
	}
	dataNode = value.(*DataNode)
	return
}

func (c *Cluster) getMetaNode(addr string) (metaNode *MetaNode, err error) {
	value, ok := c.metaNodes.Load(addr)
	if !ok {
		err = MetaNodeNotFound
	}
	metaNode = value.(*MetaNode)
	return
}

func (c *Cluster) dataNodeOffLine(dataNode *DataNode) {
	msg := fmt.Sprintf("action[dataNodeOffLine], Node[%v] OffLine", dataNode.HttpAddr)
	log.LogWarn(msg)
	for _, ns := range c.namespaces {
		for _, vg := range ns.volGroups.volGroups {
			c.volOffline(dataNode.HttpAddr, vg, DataNodeOfflineInfo)
		}
	}
	c.dataNodes.Delete(dataNode.HttpAddr)

}

func (c *Cluster) volOffline(offlineAddr string, vg *VolGroup, errMsg string) {
	var (
		newHosts []string
		newAddr  string
		msg      string
		tasks    []*proto.AdminTask
		task     *proto.AdminTask
		err      error
		dataNode *DataNode
	)
	vg.Lock()
	defer vg.Unlock()
	if ok := vg.isInPersistenceHosts(offlineAddr); !ok {
		return
	}

	if err = vg.hasMissOne(); err != nil {
		goto errDeal
	}
	if err = vg.canOffLine(offlineAddr); err != nil {
		goto errDeal
	}
	vg.generatorVolOffLineLog(offlineAddr)

	if dataNode, err = c.getDataNode(vg.PersistenceHosts[0]); err != nil {
		goto errDeal
	}
	if newHosts, err = c.getAvailDataNodeHosts(dataNode.RackName, vg.PersistenceHosts, 1); err != nil {
		goto errDeal
	}
	if err = vg.removeVolHosts(offlineAddr); err != nil {
		goto errDeal
	}
	newAddr = newHosts[0]
	if err = vg.addVolHosts(newAddr); err != nil {
		goto errDeal
	}
	vg.volOffLineInMem(offlineAddr)
	vg.checkAndRemoveMissVol(offlineAddr)
	task = proto.NewAdminTask(OpCreateVol, offlineAddr, newCreateVolRequest(vg.volType, vg.VolID))
	tasks = make([]*proto.AdminTask, 0)
	tasks = append(tasks, task)
	c.putDataNodeTasks(tasks)
	goto errDeal
errDeal:
	msg = fmt.Sprintf(errMsg+" vol:%v  on Node:%v  "+
		"DiskError  TimeOut Report Then Fix It on newHost:%v   Err:%v , PersistenceHosts:%v  ",
		vg.VolID, offlineAddr, newAddr, err, vg.PersistenceHosts)
	log.LogWarn(msg)
}

func (c *Cluster) metaNodeOffLine(metaNode *MetaNode) {

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

	c.namespaces[name] = ns
	if err = c.CreateMetaPartition(name, 0, DefaultMaxMetaPartitionRange); err != nil {
		delete(c.namespaces, name)
		goto errDeal
	}
	return
errDeal:
	err = fmt.Errorf("action[createNamespace], name:%v, err:%v ", name, err.Error())
	log.LogError(err.Error())
	return
}

func (c *Cluster) CreateMetaPartition(nsName string, start, end uint64) (err error) {
	var (
		ns      *NameSpace
		mp      *MetaPartition
		hosts   []string
		groupId uint64
		peers   []proto.Peer
	)
	ns, ok := c.namespaces[nsName]
	if !ok {
		err = elementNotFound(nsName)
		return
	}
	if groupId, err = c.getMaxID(); err != nil {
		return
	}
	mp = NewMetaPartition(groupId, start, end)
	if hosts, peers, err = c.ChooseTargetMetaHosts(int(ns.mpReplicaNum)); err != nil {
		return
	}
	mp.PersistenceHosts = hosts
	mp.peers = peers
	//todo sync namespace and metaGroup
	ns.AddMetaPartition(mp)
	c.putMetaNodeTasks(mp.generateCreateMetaPartitionTasks(nil))
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
		return
	}
	peers = append(peers, masterPeer...)
	hosts = append(hosts, masterAddr[0])
	otherReplica := replicaNum - 1
	if otherReplica == 0 {
		return
	}
	metaNode, err := c.getMetaNode(masterAddr[0])
	if err != nil {
		return
	}
	if slaveAddrs, slavePeers, err = c.getAvailMetaNodeHosts(metaNode.RackName, hosts, otherReplica); err != nil {
		return
	}
	hosts = append(hosts, slaveAddrs...)
	peers = append(peers, slavePeers...)
	if len(hosts) != replicaNum {
		return nil, nil, NoAnyMetaNodeForCreateVol
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
