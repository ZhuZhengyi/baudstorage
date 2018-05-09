package master

import (
	"fmt"
	"github.com/tiglabs/baudstorage/proto"
	"github.com/tiglabs/baudstorage/util/log"
	"sync"
	"time"
)

type Cluster struct {
	Name              string
	namespaces        map[string]*NameSpace
	metaRangeReplicas uint8
	dataNodes         sync.Map
	metaNodes         sync.Map
	createVolLock     sync.Mutex
	addZoneLock       sync.Mutex
	cfg               *ClusterConfig
}

func NewCluster(name string) (c *Cluster) {
	c = new(Cluster)
	c.Name = name
	c.cfg = NewClusterConfig()
	c.startCheckVolGroups()
	c.startCheckBackendLoadVolGroups()
	c.startCheckReleaseVolGroups()
	c.startCheckHearBeat()
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

func (c *Cluster) addMetaNode(nodeAddr string) (err error) {
	var metaNode *MetaNode
	if _, ok := c.metaNodes.Load(nodeAddr); ok {
		err = hasExist(nodeAddr)
		goto errDeal
	}
	metaNode = NewMetaNode(nodeAddr)
	//todo sync node by raft
	c.metaNodes.Store(nodeAddr, metaNode)
	return
errDeal:
	err = fmt.Errorf("action[addMetaNode],metaNodeAddr:%v err:%v ", nodeAddr, err.Error())
	log.LogWarn(err.Error())
	return err
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

func (c *Cluster) getNamespace(nsName string) (ns *NameSpace, err error) {
	ns, ok := c.namespaces[nsName]
	if !ok {
		err = NamespaceNotFound
	}
	return
}

func (c *Cluster) createVolGroup(nsName string) (vg *VolGroup, err error) {
	var (
		ns    *NameSpace
		volID uint64
		tasks []*proto.AdminTask
	)
	c.createVolLock.Lock()
	defer c.createVolLock.Unlock()
	if ns, err = c.getNamespace(nsName); err != nil {
		goto errDeal
	}

	if volID, err = c.getMaxVolID(); err != nil {
		goto errDeal
	}
	//volID++
	vg = newVolGroup(volID, c.cfg.replicaNum)
	if err = vg.ChooseTargetHosts(c); err != nil {
		goto errDeal
	}
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

func (c *Cluster) getMaxVolID() (volID uint64, err error) {
	//todo getVolID from raft
	if err != nil {
		goto errDeal
	}
	return
errDeal:
	err = fmt.Errorf("action[getMaxVolID], Err:%v ", err.Error())
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
		ns.volGroups.dataNodeOffline(dataNode.HttpAddr)
		c.dataNodes.Delete(dataNode.HttpAddr)
	}

}

func (c *Cluster) volOffline(offlineAddr string, vg *VolGroup, errMsg string) {
	var (
		newHosts []string
		newAddr  string
		msg      string
		tasks    []*proto.AdminTask
		task     *proto.AdminTask
		err      error
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

	if newHosts, err = c.getAvailDataNodeHosts(vg.PersistenceHosts, 1); err != nil {
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
		mg *MetaGroup
	)
	if _, ok := c.namespaces[name]; ok {
		err = hasExist(name)
		goto errDeal
	}
	ns = NewNameSpace(name, replicaNum)
	mg = NewMetaGroup(0, DefaultMetaTabletRange-1)
	if err = mg.ChooseTargetHosts(c); err != nil {
		goto errDeal
	}
	mg.createRange()
	//todo sync namespace and metaGroup

	c.putMetaNodeTasks(mg.generateCreateMetaGroupTasks())
	ns.AddMetaGroup(mg)
	return
errDeal:
	err = fmt.Errorf("action[createNamespace], name:%v, err:%v ", name, err.Error())
	log.LogError(err.Error())
	return
}

func (c *Cluster) DataNodeCount() (len int) {

	c.dataNodes.Range(func(key, value interface{}) bool {
		len++
		return true
	})
	return
}
