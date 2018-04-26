package master

import (
	"fmt"
	"github.com/tiglabs/baudstorage/proto"
	"github.com/tiglabs/baudstorage/util/log"
	"strings"
	"sync"
)

type Cluster struct {
	Name              string
	vols              *VolMap
	namespaces        map[string]*NameSpace
	volReplicas       uint8
	metaRangeReplicas uint8
	topology          *Topology
	createVolLock     sync.Mutex
	addZoneLock       sync.Mutex
}

func NewCluster(name string) (c *Cluster) {
	c = new(Cluster)
	c.Name = name
	c.vols = NewVolMap()
	c.topology = NewTopology()
	return
}

func (c *Cluster) addMetaNode(nodeAddr, zoneName string) (err error) {
	var metaNode *MetaNode
	if _, ok := c.topology.metaNodeMap[nodeAddr]; ok {
		err = hasExist(nodeAddr)
		goto errDeal
	}

	if _, err = c.topology.getZone(zoneName); err == nil {
		err = hasExist(zoneName)
		goto errDeal
	}
	metaNode = NewMetaNode(nodeAddr, zoneName)
	//todo sync node by raft

	if err = c.topology.addMetaNode(metaNode); err != nil {
		metaNode.clean()
		goto errDeal
	}
	return
errDeal:
	err = fmt.Errorf("action[addMetaNode],zoneName:%v,metaNodeAddr:%v err:%v ", zoneName, nodeAddr, err.Error())
	log.LogWarn(err.Error())
	return err
}

func (c *Cluster) addDataNode(nodeAddr, zoneName string) (err error) {
	var dataNode *DataNode
	if _, ok := c.topology.metaNodeMap[nodeAddr]; ok {
		err = hasExist(nodeAddr)
		goto errDeal
	}

	if _, err = c.topology.getZone(zoneName); err == nil {
		err = hasExist(zoneName)
		goto errDeal
	}
	dataNode = NewDataNode(nodeAddr, zoneName)
	//todo sync node by raft

	if err = c.topology.addDataNode(dataNode); err != nil {
		dataNode.clean()
		goto errDeal
	}
	return
errDeal:
	err = fmt.Errorf("action[addMetaNode],zoneName:%v,metaNodeAddr:%v err:%v ", zoneName, nodeAddr, err.Error())
	log.LogWarn(err.Error())
	return err
}

func (c *Cluster) getVolsView() (body []byte, err error) {
	body, err = c.vols.updateVolResponseCache(NoNeedUpdateVolResponse, 0)

	return
}

func (c *Cluster) getNamespace() (body []byte, err error) {
	body, err = c.vols.updateVolResponseCache(NoNeedUpdateVolResponse, 0)

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
	vg = newVolGroup(volID, c.volReplicas)
	if err = vg.SelectHosts(c); err != nil {
		goto errDeal
	}
	//todo sync and persistence hosts to other node in the cluster
	tasks = vg.generateCreateVolGroupTasks()
	c.putDataNodeTasks(tasks)
	c.vols.putVol(vg)

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
	return c.vols.getVol(volID)
}

func (c *Cluster) getDataNodeFromCluster(addr string) (dataNode *DataNode, zone *Zone, err error) {
	dataNode, _, zone, err = c.topology.getDataNode(addr)
	return
}

func (c *Cluster) getMetaNodeFromCluster(addr string) (metaNode *MetaNode, zone *Zone, err error) {
	metaNode, _, zone, err = c.topology.getMetaNode(addr)
	return
}

func (c *Cluster) loadVolAndCheckResponse(v *VolGroup, isRecover bool) {
	go func() {
		c.processLoadVol(v, isRecover)
	}()
}

func (c *Cluster) processLoadVol(v *VolGroup, isRecover bool) {

}

func (c *Cluster) dataNodeOffLine(dataNode *DataNode) {
	msg := fmt.Sprintf("action[dataNodeOffLine], Node[%v] OffLine", dataNode.HttpAddr)
	log.LogWarn(msg)
	c.vols.dataNodeOffline(dataNode.HttpAddr)
	if err := c.topology.deleteDataNode(dataNode); err != nil {
		log.LogError(fmt.Sprintf("action[dataNodeOffLine],nodeAddr:%v,err:%v", dataNode.HttpAddr, err))
	}

}

func (c *Cluster) metaNodeOffLine(metaNode *MetaNode) {

}

func (c *Cluster) addZone(zoneName string) (err error) {
	c.addZoneLock.Lock()
	defer c.addZoneLock.Unlock()
	if _, err = c.topology.getZone(zoneName); err == nil {
		err = hasExist(zoneName)
		goto errDeal
	}
	//todo sync zone by raft
	c.putRegionAndZoneToMem(zoneName)
	return nil
errDeal:
	err = fmt.Errorf("action[addZone], zoneName:%v, err:%v ", zoneName, err.Error())
	log.LogError(err.Error())
	return
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
	if err = mg.SelectHosts(c); err != nil {
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

func (c *Cluster) putRegionAndZoneToMem(zoneName string) {
	var (
		r          *Region
		zone       *Zone
		arr        []string
		regionName string
		err        error
	)
	arr = strings.Split(zoneName, UnderlineSeparator)
	regionName = arr[0]
	if r, err = c.topology.getRegion(regionName); err != nil {
		r = NewRegion(regionName)
		c.topology.putRegion(r)
	}
	if zone, err = c.topology.getZone(zoneName); err != nil {
		zone = NewZone(zoneName)
		c.topology.putZone(zone)
	}
}

func (c *Cluster) putDataNodeTasks(tasks []*proto.AdminTask) {

	for _, t := range tasks {
		if t == nil {
			continue
		}
		if node, _, err := c.getDataNodeFromCluster(t.OperatorAddr); err != nil {
			log.LogWarn(fmt.Sprintf("action[putTasks],nodeAddr:%v,taskID:%v,err:%v", node.HttpAddr, t.ID, err.Error()))
		} else {
			node.sender.PutTask(t)

		}
	}
}

func (c *Cluster) putMetaNodeTasks(tasks []*proto.AdminTask) {

	for _, t := range tasks {
		if t == nil {
			continue
		}
		if node, _, err := c.getMetaNodeFromCluster(t.OperatorAddr); err != nil {
			log.LogWarn(fmt.Sprintf("action[putTasks],nodeAddr:%v,taskID:%v,err:%v", node.Addr, t.ID, err.Error()))
		} else {
			node.sender.PutTask(t)

		}
	}
}
