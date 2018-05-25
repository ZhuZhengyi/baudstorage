package master

import (
	"fmt"
	"github.com/tiglabs/baudstorage/proto"
	"github.com/tiglabs/baudstorage/util/log"
	"math/rand"
	"sort"
	"time"
)

type NodeTab struct {
	Carry  float64
	Weight float64
	Ptr    Node
	Id     uint64
}

type Node interface {
	SetCarry(carry float64)
	SelectNodeForWrite()
}

type NodeTabArrSorterByCarry []*NodeTab

func (nodeTabs NodeTabArrSorterByCarry) Len() int {
	return len(nodeTabs)
}

func (nodeTabs NodeTabArrSorterByCarry) Less(i, j int) bool {
	return nodeTabs[i].Carry > nodeTabs[j].Carry
}

func (nodeTabs NodeTabArrSorterByCarry) Swap(i, j int) {
	nodeTabs[i], nodeTabs[j] = nodeTabs[j], nodeTabs[i]
}

func (nodeTabs NodeTabArrSorterByCarry) SetNodeTabCarry(availCarryCount, replicaNum int) {
	if availCarryCount >= replicaNum {
		return
	}
	for availCarryCount < replicaNum {
		availCarryCount = 0
		for _, nt := range nodeTabs {
			carry := nt.Carry + nt.Weight
			if carry > 10.0 {
				carry = 10.0
			}
			nt.Carry = carry
			nt.Ptr.SetCarry(carry)
			if carry > 1.0 {
				availCarryCount++
			}
		}
	}
}

func (c *Cluster) GetDataNodeMaxTotal() (maxTotal uint64) {
	c.dataNodes.Range(func(key, value interface{}) bool {
		dataNode := value.(*DataNode)
		if dataNode.Total > maxTotal {
			maxTotal = dataNode.Total
		}
		return true
	})
	return
}

func (c *Cluster) getAvailDataNodeHosts(excludeRack string, excludeHosts []string, replicaNum int) (newHosts []string, err error) {
	orderHosts := make([]string, 0)
	newHosts = make([]string, 0)
	if replicaNum == 0 {
		return
	}

	maxTotal := c.GetDataNodeMaxTotal()
	nodeTabs, availCarryCount := c.GetAvailCarryDataNodeTab(maxTotal, excludeRack, excludeHosts)
	if len(nodeTabs) < replicaNum {
		err = fmt.Errorf(GetAvailDataNodeHostsErr+" err:%v ,ActiveNodeCount:%v  MatchNodeCount:%v  ",
			NoHaveAnyDataNodeToWrite, c.DataNodeCount(), len(nodeTabs))
		return
	}

	nodeTabs.SetNodeTabCarry(availCarryCount, replicaNum)
	sort.Sort(nodeTabs)

	for i := 0; i < replicaNum; i++ {
		node := nodeTabs[i].Ptr.(*DataNode)
		node.SelectNodeForWrite()
		orderHosts = append(orderHosts, node.HttpAddr)
	}

	if newHosts, err = c.DisOrderArray(orderHosts); err != nil {
		err = fmt.Errorf(GetAvailDataNodeHostsErr+"err:%v  orderHosts is nil", err.Error())
		return
	}
	return
}

func (c *Cluster) GetAvailCarryDataNodeTab(maxTotal uint64, excludeRack string, excludeHosts []string) (nodeTabs NodeTabArrSorterByCarry, availCount int) {
	nodeTabs = make(NodeTabArrSorterByCarry, 0)
	c.dataNodes.Range(func(key, value interface{}) bool {
		dataNode := value.(*DataNode)
		if dataNode.RackName == excludeRack {
			return true
		}
		if contains(excludeHosts, dataNode.HttpAddr) == true {
			return true
		}
		if dataNode.IsWriteAble() == false {
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
			nt.Weight = (float64)(maxTotal-dataNode.Used) / (float64)(maxTotal)
		}
		nt.Ptr = dataNode
		nodeTabs = append(nodeTabs, nt)

		return true
	})

	return
}

func (c *Cluster) GetMetaNodeMaxTotal() (maxTotal uint64) {
	c.metaNodes.Range(func(key, value interface{}) bool {
		metaNode := value.(*MetaNode)
		if metaNode.Total > maxTotal {
			maxTotal = metaNode.Total
		}
		return true
	})
	return
}

func (c *Cluster) getAvailMetaNodeHosts(excludeRack string, excludeHosts []string, replicaNum int) (newHosts []string, peers []proto.Peer, err error) {
	orderHosts := make([]string, 0)
	newHosts = make([]string, 0)
	peers = make([]proto.Peer, 0)
	if replicaNum == 0 {
		return
	}

	maxTotal := c.GetMetaNodeMaxTotal()
	nodeTabs, availCarryCount := c.GetAvailCarryMetaNodeTab(maxTotal, excludeRack, excludeHosts)
	if len(nodeTabs) < replicaNum {
		err = fmt.Errorf(GetAvailMetaNodeHostsErr+" err:%v ,ActiveNodeCount:%v  MatchNodeCount:%v  ",
			NoHaveAnyMetaNodeToWrite, c.DataNodeCount(), len(nodeTabs))
		return
	}

	nodeTabs.SetNodeTabCarry(availCarryCount, replicaNum)
	sort.Sort(nodeTabs)

	for i := 0; i < replicaNum; i++ {
		node := nodeTabs[i].Ptr.(*MetaNode)
		node.SelectNodeForWrite()
		orderHosts = append(orderHosts, node.Addr)
		peer := proto.Peer{ID: node.id, Addr: node.Addr}
		peers = append(peers, peer)
	}

	if newHosts, err = c.DisOrderArray(orderHosts); err != nil {
		err = fmt.Errorf(GetAvailMetaNodeHostsErr+"err:%v  orderHosts is nil", err.Error())
		return
	}
	return
}

func (c *Cluster) GetAvailCarryMetaNodeTab(maxTotal uint64, excludeRack string, excludeHosts []string) (nodeTabs NodeTabArrSorterByCarry, availCount int) {
	nodeTabs = make(NodeTabArrSorterByCarry, 0)
	c.metaNodes.Range(func(key, value interface{}) bool {
		metaNode := value.(*MetaNode)
		//if metaNode.RackName == excludeRack {
		//	return true
		//}
		if contains(excludeHosts, metaNode.Addr) == true {
			return true
		}
		if metaNode.IsWriteAble() == false {
			return true
		}
		if metaNode.IsAvailCarryNode() == true {
			availCount++
		}
		nt := new(NodeTab)
		nt.Carry = metaNode.carry
		if metaNode.Used < 0 {
			nt.Weight = 1.0
		} else {
			nt.Weight = (float64)(maxTotal-metaNode.Used) / (float64)(maxTotal)
		}
		nt.Ptr = metaNode
		nodeTabs = append(nodeTabs, nt)

		return true
	})

	return
}

func (c *Cluster) DisOrderArray(oldHosts []string) (newHosts []string, err error) {
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
