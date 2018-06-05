package master

import (
	"fmt"
	"github.com/tiglabs/baudstorage/util/log"
	"math/rand"
	"sort"
	"sync"
	"time"
)

type Topology struct {
	rackIndex int
	rackMap   map[string]*Rack
	racks     []string
	rackLock  sync.Mutex
}

func NewTopology() (t *Topology) {
	t = new(Topology)
	t.rackMap = make(map[string]*Rack)
	t.racks = make([]string, 0)
	return
}

type Rack struct {
	name      string
	dataNodes sync.Map
	sync.RWMutex
}

func NewRack(name string) (rack *Rack) {
	return &Rack{name: name}
}

func (c *Cluster) isSingleRack() bool {
	return len(c.t.rackMap) == 1
}

func (t *Topology) getRack(name string) (rack *Rack, err error) {
	t.rackLock.Lock()
	defer t.rackLock.Unlock()
	rack, ok := t.rackMap[name]
	if !ok {
		return nil, RackNotFound
	}
	return
}

func (t *Topology) putRack(rack *Rack) {
	t.rackLock.Lock()
	defer t.rackLock.Unlock()
	t.rackMap[rack.name] = rack
	if ok := t.isExist(rack.name); !ok {
		t.racks = append(t.racks, rack.name)
	}
}

func (t *Topology) isExist(rackName string) (ok bool) {
	for _, name := range t.racks {
		if name == rackName {
			ok = true
			return
		}
	}
	return
}

func (t *Topology) removeRack(name string) {
	t.rackLock.Lock()
	defer t.rackLock.Unlock()
	delete(t.rackMap, name)
}

func (t *Topology) putDataNode(dataNode *DataNode) {
	rack, err := t.getRack(dataNode.RackName)
	if err != nil {
		rack = NewRack(dataNode.RackName)
		t.putRack(rack)
	}
	rack.PutDataNode(dataNode)
}

func (t *Topology) allocRacks(replicaNum int, excludeRack []string) (racks []*Rack, err error) {
	racks = make([]*Rack, 0)
	if excludeRack == nil {
		excludeRack = make([]string, 0)
	}
	err = NoRackForCreateDataPartition
	if len(t.racks) == 1 {
		for _, rack := range t.rackMap {
			racks = append(racks, rack)
		}
		return
	}

	for i := 0; i < len(t.racks); i++ {
		if t.rackIndex >= len(t.racks) {
			t.rackIndex = 0
		}
		rName := t.racks[t.rackIndex]
		if contains(excludeRack, rName) {
			continue
		}
		t.rackIndex++
		var rack *Rack
		if rack, err = t.getRack(t.racks[t.rackIndex]); err != nil {
			continue
		}
		if rack.canWrite(1) {
			racks = append(racks, rack)
		}
		if len(racks) >= int(replicaNum) {
			break
		}
	}
	if len(racks) == 0 {
		log.LogError(fmt.Sprintf("action[allocRacks],err:%v", NoRackForCreateDataPartition))
		return nil, NoRackForCreateDataPartition
	}
	if len(racks) > int(replicaNum) {
		racks = racks[:int(replicaNum)]
	}
	err = nil
	return
}

func (rack *Rack) canWrite(replicaNum uint8) (can bool) {
	rack.RLock()
	defer rack.RUnlock()
	var leastAlive uint8
	rack.dataNodes.Range(func(addr, value interface{}) bool {
		dataNode := value.(*DataNode)
		if dataNode.isActive == true && dataNode.IsWriteAble() == true {
			leastAlive++
		}
		if leastAlive >= replicaNum {
			can = true
			return false
		}
		return true
	})
	return
}

func (rack *Rack) PutDataNode(dataNode *DataNode) {
	rack.dataNodes.Store(dataNode.Addr, dataNode)
}

func (rack *Rack) GetDataNode(addr string) (dataNode *DataNode, err error) {
	value, ok := rack.dataNodes.Load(addr)
	if !ok {
		return nil, DataNodeNotFound
	}
	dataNode = value.(*DataNode)
	return
}

func (rack *Rack) RemoveDataNode(addr string) {
	rack.dataNodes.Delete(addr)
}

func (rack *Rack) GetDataNodeMaxTotal() (maxTotal uint64) {
	rack.dataNodes.Range(func(key, value interface{}) bool {
		dataNode := value.(*DataNode)
		if dataNode.Total > maxTotal {
			maxTotal = dataNode.Total
		}
		return true
	})
	return
}

func (rack *Rack) getAvailDataNodeHosts(excludeHosts []string, replicaNum int) (newHosts []string, err error) {
	orderHosts := make([]string, 0)
	newHosts = make([]string, 0)
	if replicaNum == 0 {
		return
	}

	maxTotal := rack.GetDataNodeMaxTotal()
	nodeTabs, availCarryCount := rack.GetAvailCarryDataNodeTab(maxTotal, excludeHosts, replicaNum)
	if len(nodeTabs) < replicaNum {
		err = NoHaveAnyDataNodeToWrite
		err = fmt.Errorf(GetAvailDataNodeHostsErr+" err:%v ,ActiveNodeCount:%v  MatchNodeCount:%v  ",
			NoHaveAnyDataNodeToWrite, rack.DataNodeCount(), len(nodeTabs))
		return
	}

	nodeTabs.SetNodeTabCarry(availCarryCount, replicaNum)
	sort.Sort(nodeTabs)

	for i := 0; i < replicaNum; i++ {
		node := nodeTabs[i].Ptr.(*DataNode)
		node.SelectNodeForWrite()
		orderHosts = append(orderHosts, node.Addr)
	}

	if newHosts, err = rack.DisOrderArray(orderHosts); err != nil {
		err = fmt.Errorf(GetAvailDataNodeHostsErr+"err:%v  orderHosts is nil", err.Error())
		return
	}
	return
}

func (rack *Rack) GetAvailCarryDataNodeTab(maxTotal uint64, excludeHosts []string, replicaNum int) (nodeTabs NodeTabArrSorterByCarry, availCount int) {
	nodeTabs = make(NodeTabArrSorterByCarry, 0)
	rack.dataNodes.Range(func(key, value interface{}) bool {
		dataNode := value.(*DataNode)
		if contains(excludeHosts, dataNode.Addr) == true {
			log.LogDebugf("contains return")
			return true
		}
		if dataNode.IsWriteAble() == false {
			log.LogDebugf("isWritable return")
			return true
		}
		if dataNode.IsAvailCarryNode() == true {
			availCount++
		}
		nt := new(NodeTab)
		nt.Carry = dataNode.carry
		if dataNode.Used < 0 {
			nt.Weight = 1.0
		} else {
			nt.Weight = float64(dataNode.RemainWeightsForCreateVol) / float64(maxTotal)
		}
		nt.Ptr = dataNode
		nodeTabs = append(nodeTabs, nt)

		return true
	})

	return
}

func (rack *Rack) DataNodeCount() (len int) {

	rack.dataNodes.Range(func(key, value interface{}) bool {
		len++
		return true
	})
	return
}

func (rack *Rack) DisOrderArray(oldHosts []string) (newHosts []string, err error) {
	var (
		newCurrPos int
	)

	if oldHosts == nil || len(oldHosts) == 0 {
		log.LogError(fmt.Sprintf("action[DisOrderArray],err:%v", DisOrderArrayErr))
		err = DisOrderArrayErr
		return
	}

	lenOldHosts := len(oldHosts)
	newHosts = make([]string, lenOldHosts)
	if lenOldHosts == 1 {
		copy(newHosts, oldHosts)
		return
	}

	for randCount := 0; randCount < lenOldHosts; randCount++ {
		remainCount := lenOldHosts - randCount
		rand.Seed(time.Now().UnixNano())
		oCurrPos := rand.Intn(remainCount)
		newHosts[newCurrPos] = oldHosts[oCurrPos]
		newCurrPos++
		oldHosts[oCurrPos] = oldHosts[remainCount-1]
	}

	return
}
