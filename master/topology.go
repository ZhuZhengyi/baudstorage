package master

import (
	"fmt"
	"github.com/tiglabs/baudstorage/util/log"
	"math/rand"
	"strings"
	"sync"
)

const (
	DataNodeType = "dataNode"
	MetaNodeType = "metaNode"
)

type Topology struct {
	dataNodeMap  map[string]*DataNode
	dataNodeLock sync.RWMutex
	metaNodeMap  map[string]*MetaNode
	metaNodeLock sync.RWMutex
	RegionMap    map[string]*Region
	regionLock   sync.RWMutex
	regionIndex  int
	regions      []*Region
	ZoneMap      map[string]*Zone
	zoneLock     sync.RWMutex
}

func NewTopology() (t *Topology) {
	t = new(Topology)
	t.dataNodeMap = make(map[string]*DataNode)
	t.metaNodeMap = make(map[string]*MetaNode)
	t.RegionMap = make(map[string]*Region)
	t.ZoneMap = make(map[string]*Zone)
	return t
}

func (t *Topology) putRegion(r *Region) {
	if _, err := t.getRegion(r.RegionName); err == nil {
		return
	}
	t.regionLock.Lock()
	defer t.regionLock.Unlock()
	t.RegionMap[r.RegionName] = r
	t.regions = append(t.regions, r)
}

func (t *Topology) putZone(zone *Zone) (err error) {
	var (
		r *Region
	)
	arr := strings.Split(zone.Name, UnderlineSeparator)
	regionName := arr[0]
	if r, err = t.getRegion(regionName); err != nil {
		return
	}
	t.zoneLock.Lock()
	defer t.zoneLock.Unlock()
	t.ZoneMap[zone.Name] = zone
	r.putZone(zone.Name)
	return
}

func (t *Topology) getZone(name string) (zone *Zone, err error) {
	var ok bool
	t.zoneLock.RLock()
	defer t.zoneLock.RUnlock()

	if zone, ok = t.ZoneMap[name]; !ok {
		err = elementNotFound(name)
		log.LogError(fmt.Sprintf("action[getZone],err:%v", err.Error()))
		return nil, ZoneNotFound
	}
	return
}

func (t *Topology) getRegion(regionName string) (r *Region, err error) {
	var ok bool
	t.regionLock.RLock()
	defer t.regionLock.RUnlock()
	r, ok = t.RegionMap[regionName]
	if !ok {
		log.LogError(fmt.Sprintf("action[getRegion],regionName:%v,err:%v", regionName, RegionNotFound))
		return nil, RegionNotFound
	}
	return
}

func (t *Topology) addMetaNode(metaNode *MetaNode) (err error) {

	var zone *Zone
	arr := strings.Split(metaNode.ZoneName, UnderlineSeparator)
	regionName := arr[0]
	if _, err = t.getRegion(regionName); err != nil {
		return
	}
	if zone, err = t.getZone(metaNode.ZoneName); err != nil {
		return
	}
	t.metaNodeLock.Lock()
	defer t.metaNodeLock.Unlock()
	t.metaNodeMap[metaNode.Addr] = metaNode
	if zone.ContainsMetaNode(metaNode.Addr) {
		return
	}
	zone.PutMetaNode(metaNode.Addr, metaNode)
	return
}

func (t *Topology) addDataNode(dataNode *DataNode) (err error) {

	var zone *Zone
	arr := strings.Split(dataNode.ZoneName, UnderlineSeparator)
	regionName := arr[0]
	if _, err = t.getRegion(regionName); err != nil {
		return
	}
	if zone, err = t.getZone(dataNode.ZoneName); err != nil {
		return
	}
	t.dataNodeLock.Lock()
	defer t.dataNodeLock.Unlock()
	t.dataNodeMap[dataNode.HttpAddr] = dataNode
	if zone.ContainsDataNode(dataNode.HttpAddr) {
		return
	}
	zone.PutDataNode(dataNode.HttpAddr, dataNode)
	return
}

func (t *Topology) getDataNode(addr string) (node *DataNode, r *Region, zone *Zone, err error) {
	var ok bool
	t.dataNodeLock.RLock()
	defer t.dataNodeLock.RUnlock()
	node, ok = t.dataNodeMap[addr]
	if !ok {
		log.LogError(fmt.Sprintf("action[getDataNode],nodeAddr:%v,err:%v", addr, DataNodeNotFound))
		err = DataNodeNotFound
		return
	}
	r, zone, err = t.getRegionAndZone(node.ZoneName)
	return
}

func (t *Topology) getRegionAndZone(zoneName string) (r *Region, zone *Zone, err error) {
	arr := strings.Split(zoneName, UnderlineSeparator)
	regionName := arr[0]
	if r, err = t.getRegion(regionName); err != nil {
		return
	}
	zone, err = t.getZone(zoneName)
	return
}

func (t *Topology) getMetaNode(addr string) (node *MetaNode, r *Region, zone *Zone, err error) {
	var ok bool
	t.metaNodeLock.RLock()
	defer t.metaNodeLock.RUnlock()
	node, ok = t.metaNodeMap[addr]
	if !ok {
		log.LogError(fmt.Sprintf("action[getMetaNode],nodeAddr:%v,err:%v", addr, MetaNodeNotFound))
		err = MetaNodeNotFound
		return
	}
	r, zone, err = t.getRegionAndZone(node.ZoneName)
	return
}

func (t *Topology) deleteDataNode(dataNode *DataNode) (err error) {
	var zone *Zone
	if zone, err = t.getZone(dataNode.ZoneName); err != nil {
		return
	}
	zone.deleteDataNode(dataNode.HttpAddr)
	dataNode.sender.exitCh <- struct{}{}
	t.dataNodeLock.Lock()
	delete(t.dataNodeMap, dataNode.HttpAddr)
	t.dataNodeLock.Unlock()
	return
}

func (t *Topology) deleteMetaNode(metaNode *MetaNode) (err error) {
	var zone *Zone
	if zone, err = t.getZone(metaNode.ZoneName); err != nil {
		return
	}
	zone.deleteMetaNode(metaNode.Addr)
	metaNode.sender.exitCh <- struct{}{}
	t.metaNodeLock.Lock()
	delete(t.metaNodeMap, metaNode.Addr)
	t.metaNodeLock.Unlock()
	return
}

func (t *Topology) allocZone(goal uint8, excludeZone []string) (zones []*Zone, err error) {
	factory := rand.Float64()
	zones = make([]*Zone, 0)
	if excludeZone == nil {
		excludeZone = make([]string, 0)
	}
	err = NoZoneForCreateVol
	if len(t.regions) == 1 {
		return t.onlySingleRegion(goal, excludeZone)
	}

	for i := 0; i < len(t.regions); i++ {
		if t.regionIndex >= len(t.regions) {
			t.regionIndex = 0
		}
		r := t.regions[t.regionIndex]
		t.regionIndex++
		if factory >= 0.1 {
			if zone := r.allocZone(1, excludeZone); zone != nil {
				zones = append(zones, zone)
				excludeZone = append(excludeZone, zone.Name)
			}
		} else {
			needGoal := int(goal) - len(zones)
			if needGoal == int(goal) {
				needGoal = int(goal)/2 + 1
			}
			if zone := r.allocZone(uint8(needGoal), excludeZone); zone != nil {
				zones = append(zones, zone)
				excludeZone = append(excludeZone, zone.Name)
			}
		}
		if len(zones) >= int(goal) {
			break
		}
	}
	if len(zones) == 0 {
		log.LogError(fmt.Sprintf("action[allocZone],err:%v", NoZoneForCreateVol))
		return nil, NoZoneForCreateVol
	}
	if len(zones) > int(goal) {
		zones = zones[:int(goal)]
	}
	err = nil
	return
}

func (t *Topology) onlySingleRegion(goal uint8, excludeZone []string) (zones []*Zone, err error) {
	zones = make([]*Zone, 0)
	if excludeZone == nil {
		excludeZone = make([]string, 0)
	}
	for i := 0; i < int(goal); i++ {
		if zone := t.regions[0].allocZone(1, excludeZone); zone != nil {
			excludeZone = append(excludeZone, zone.Name)
			zones = append(zones, zone)
		}
	}

	if len(zones) == 0 {
		log.LogError(fmt.Sprintf("action[onlySingleRegion],err:%v", NoZoneForCreateVol))
		return nil, NoZoneForCreateVol
	}

	return
}

type Region struct {
	RegionName string
	Zones      []string
	index      int
}

func NewRegion(name string) (r *Region) {
	r = new(Region)
	r.RegionName = name
	r.Zones = make([]string, 0)
	return
}

func (r *Region) putZone(zoneName string) {
	r.Zones = append(r.Zones, zoneName)
	return
}

func (r *Region) clean() {
	r.Zones = make([]string, 0)
	r.index = 0
}

func contains(name string, arrName []string) (in bool) {
	for _, n := range arrName {
		if name == n {
			in = true
			break
		}
	}
	return
}

func (r *Region) allocZone(goal uint8, excludeZone []string) *Zone {
	for index := 0; index < len(r.Zones); index++ {
		if r.index >= len(r.Zones) {
			r.index = 0
		}
		var zone *Zone

		if contains(zone.Name, excludeZone) {
			continue
		}
		r.index++
		if zone.dataNodeCanWrite(goal) {
			return zone
		}
	}

	return nil
}

type Zone struct {
	Name      string
	DataNodes map[string]*DataNode
	MetaNodes map[string]*MetaNode
	sync.RWMutex
}

func NewZone(name string) (zone *Zone) {
	zone = new(Zone)
	zone.Name = name
	zone.DataNodes = make(map[string]*DataNode, 0)
	zone.MetaNodes = make(map[string]*MetaNode, 0)

	return
}

func (zone *Zone) ContainsMetaNode(addr string) (exist bool) {
	return zone.containsNode(addr, MetaNodeType)
}

func (zone *Zone) ContainsDataNode(addr string) (exist bool) {
	return zone.containsNode(addr, DataNodeType)
}

func (zone *Zone) containsNode(addr string, nodeType string) (exist bool) {
	zone.RLock()
	defer zone.RUnlock()
	switch nodeType {
	case DataNodeType:
		for nodeAddr := range zone.DataNodes {
			if addr == nodeAddr {
				return true
			}
		}
	case MetaNodeType:
		for nodeAddr := range zone.MetaNodes {
			if addr == nodeAddr {
				return true
			}
		}
	}
	return false
}

func (zone *Zone) PutMetaNode(addr string, metaNode *MetaNode) {
	zone.Lock()
	defer zone.Unlock()
	zone.MetaNodes[addr] = metaNode
}

func (zone *Zone) PutDataNode(addr string, dataNode *DataNode) {
	zone.Lock()
	defer zone.Unlock()
	zone.DataNodes[addr] = dataNode
}

func (zone *Zone) deleteDataNode(addr string) {
	zone.Lock()
	defer zone.Unlock()
	delete(zone.DataNodes, addr)
}

func (zone *Zone) deleteMetaNode(addr string) {
	zone.Lock()
	defer zone.Unlock()
	delete(zone.MetaNodes, addr)
}

func (zone *Zone) dataNodeCanWrite(goal uint8) (can bool) {
	zone.RLock()
	defer zone.RUnlock()
	var (
		leastAlive uint8
		node       *DataNode
	)
	for _, node = range zone.DataNodes {
		if node.isActive == true && node.IsWriteAble() == true {
			leastAlive++
		}
		if leastAlive >= goal {
			can = true
			break
		}
	}

	return
}
