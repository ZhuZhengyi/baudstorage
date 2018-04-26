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

type NodeTabSorterByCarry []*NodeTab

func (sts NodeTabSorterByCarry) Len() int {
	return len(sts)
}

func (sts NodeTabSorterByCarry) Less(i, j int) bool {
	return sts[i].Carry > sts[j].Carry
}

func (sts NodeTabSorterByCarry) Swap(i, j int) {
	sts[i], sts[j] = sts[j], sts[i]
}

func (sts NodeTabSorterByCarry) SetNodeTabCarry(availCarryCount, demand int) {
	if availCarryCount >= demand {
		return
	}
	for availCarryCount < demand {
		availCarryCount = 0
		for _, nt := range sts {
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

func (c *Cluster) getAvailHostExcludeSpecify(specifyAddrsPtr *[]string, demand int) (newHosts []string, err error) {
	orderHosts := make([]string, 0)
	newHosts = make([]string, 0)
	if demand == 0 {
		return
	}

	maxTotal := c.GetMaxTotal()
	nodeTabs, availCarryCount := c.GetAvailCarryNodeTab(maxTotal, specifyAddrsPtr)
	if len(nodeTabs) < demand {
		err = fmt.Errorf(GetAvailHostExcludeSpecifyErr+" err:%v ,ActiveNodeCount:%v  MatchNodeCount:%v  ",
			ClusterNotHaveAnyNodeToWrite, c.DataNodeCount(), len(nodeTabs))
		goto errDeal
	}

	nodeTabs.SetNodeTabCarry(availCarryCount, demand)
	sort.Sort(nodeTabs)

	for i := 0; i < demand; i++ {
		node := nodeTabs[i].Ptr
		node.SelectNodeForWrite()
		orderHosts = append(orderHosts, node.HttpAddr)
	}

	if newHosts, err = c.DisOrderArray(&orderHosts); err != nil {
		err = fmt.Errorf(GetAvailHostExcludeSpecifyErr+"err:%v  orderHosts is nil", err.Error())
		goto errDeal
	}

	return
errDeal:
	return
}

func AddrIsInSpecifyAddr(specifyAddrsPtr *[]string, addr string) (ok bool) {
	if specifyAddrsPtr == nil {
		return
	}

	specifyAddrs := *(specifyAddrsPtr)
	if specifyAddrs == nil || len(specifyAddrs) == 0 {
		return
	}

	for _, specifyAddr := range specifyAddrs {
		if specifyAddr == addr {
			ok = true
			break
		}
	}

	return
}

func (c *Cluster) GetAvailCarryNodeTab(maxTotal uint64, specifyAddrsPtr *[]string) (nodeTabs NodeTabSorterByCarry, availCount int) {
	nodeTabs = make(NodeTabSorterByCarry, 0)

	c.dataNodes.Range(func(key, value interface{}) bool {
		dataNode := value.(*DataNode)
		if AddrIsInSpecifyAddr(specifyAddrsPtr, dataNode.HttpAddr) == true {
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

func (c *Cluster) DisOrderArray(oldHostsPtr *[]string) (newHosts []string, err error) {
	var (
		newCurrPos int
	)

	if oldHostsPtr == nil || *oldHostsPtr == nil || len(*oldHostsPtr) == 0 {
		log.LogError(fmt.Sprintf("action[DisOrderArray],err:%v", DisOrderArrayErr))
		err = DisOrderArrayErr
		return
	}

	oldHosts := *oldHostsPtr
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
