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
	volGroups         *VolGroupMap
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
	c.volGroups = NewVolMap()
	c.cfg = NewClusterConfig()
	c.startCheckVolGroups()
	c.startCheckBackendLoadVolGroups()
	c.startCheckReleaseVolGroups()
	return
}

func (c *Cluster) startCheckVolGroups() {
	go func() {
		for {
			if c.volGroups != nil {
				c.checkVolGroups()
			}
			time.Sleep(time.Second * time.Duration(c.cfg.CheckVolIntervalSeconds))
		}
	}()
}

func (c *Cluster) startCheckBackendLoadVolGroups() {
	go func() {
		for {
			if c.volGroups != nil {
				c.backendLoadVolGroup(c.cfg.everyLoadVolCount, c.cfg.LoadVolFrequencyTime)
			}
			time.Sleep(time.Second)
		}
	}()
}

func (c *Cluster) startCheckReleaseVolGroups() {
	go func() {
		for {
			if c.volGroups != nil {
				c.processReleaseVolAfterLoadVolGroup()
			}
			time.Sleep(time.Second * DefaultReleaseVolInternalSeconds)
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
	body, err = c.volGroups.updateVolResponseCache(NoNeedUpdateVolResponse, 0)

	return
}

func (c *Cluster) getNamespace() (body []byte, err error) {
	body, err = c.volGroups.updateVolResponseCache(NoNeedUpdateVolResponse, 0)

	return
}

func (c *Cluster) createVolGroup() (vg *VolGroup, err error) {
	var (
		volID uint64
		tasks []*proto.AdminTask
	)
	c.createVolLock.Lock()
	defer c.createVolLock.Unlock()
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
	c.volGroups.putVol(vg)

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

func (c *Cluster) getVolByVolID(volID uint64) (vol *VolGroup, err error) {
	return c.volGroups.getVol(volID)
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
	c.volGroups.dataNodeOffline(dataNode.HttpAddr)
	c.dataNodes.Delete(dataNode.HttpAddr)
}

func (c *Cluster) metaNodeOffLine(metaNode *MetaNode) {

}

func (c *Cluster) createNamespace(name string) (err error) {
	var (
		ns *NameSpace
		mg *MetaGroup
	)
	if _, ok := c.namespaces[name]; ok {
		err = hasExist(name)
		goto errDeal
	}
	ns = NewNameSpace(name)
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
