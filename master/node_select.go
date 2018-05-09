package master

import (
	"fmt"
	"github.com/tiglabs/baudstorage/util/log"
	"math/rand"
	"sort"
	"time"
)

type NodeTab struct {
	Carry  float64
	Weight float64
	Ptr    *DataNode
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

func (nodeTabs NodeTabArrSorterByCarry) SetNodeTabCarry(availCarryCount, demand int) {
	if availCarryCount >= demand {
		return
	}
	for availCarryCount < demand {
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

func (c *Cluster) GetMaxTotal() (maxTotal uint64) {
	c.dataNodes.Range(func(key, value interface{}) bool {
		dataNode := value.(*DataNode)
		if dataNode.Total > maxTotal {
			maxTotal = dataNode.Total
		}
		return true
	})
	return
}

func (c *Cluster) getAvailDataNodeHosts(excludeHosts []string, demand int) (newHosts []string, err error) {
	orderHosts := make([]string, 0)
	newHosts = make([]string, 0)
	if demand == 0 {
		return
	}

	maxTotal := c.GetMaxTotal()
	nodeTabs, availCarryCount := c.GetAvailCarryNodeTab(maxTotal, excludeHosts)
	if len(nodeTabs) < demand {
		err = fmt.Errorf(GetAvailDataNodeHostsErr+" err:%v ,ActiveNodeCount:%v  MatchNodeCount:%v  ",
			NoHaveAnyDataNodeToWrite, c.DataNodeCount(), len(nodeTabs))
		return
	}

	nodeTabs.SetNodeTabCarry(availCarryCount, demand)
	sort.Sort(nodeTabs)

	for i := 0; i < demand; i++ {
		node := nodeTabs[i].Ptr
		node.SelectNodeForWrite()
		orderHosts = append(orderHosts, node.HttpAddr)
	}

	if newHosts, err = c.DisOrderArray(orderHosts); err != nil {
		err = fmt.Errorf(GetAvailDataNodeHostsErr+"err:%v  orderHosts is nil", err.Error())
		return
	}
	return
}

func AddrIsInArr(arr []string, addr string) (ok bool) {
	if arr == nil {
		return
	}

	if arr == nil || len(arr) == 0 {
		return
	}

	for _, specifyAddr := range arr {
		if specifyAddr == addr {
			ok = true
			break
		}
	}

	return
}

func (c *Cluster) GetAvailCarryNodeTab(maxTotal uint64, excludeHosts []string) (nodeTabs NodeTabArrSorterByCarry, availCount int) {
	nodeTabs = make(NodeTabArrSorterByCarry, 0)

	c.dataNodes.Range(func(key, value interface{}) bool {
		dataNode := value.(*DataNode)
		if AddrIsInArr(excludeHosts, dataNode.HttpAddr) == true {
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
