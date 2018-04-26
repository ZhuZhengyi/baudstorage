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

func (zone *Zone) GetMaxTotal() (maxTotal uint64) {
	for _, dataNode := range zone.DataNodes {
		if dataNode.Total > maxTotal {
			maxTotal = dataNode.Total
		}
	}

	return
}

func (zone *Zone) getAvailHostExcludeSpecify(specifyAddrsPtr *[]string, demand int) (newHosts []string, err error) {
	orderHosts := make([]string, 0)
	newHosts = make([]string, 0)
	zone.RLock()
	defer zone.RUnlock()
	if demand == 0 {
		return
	}

	maxTotal := zone.GetMaxTotal()
	nodeTabs, availCarryCount := zone.GetAvailCarryNodeTab(maxTotal, specifyAddrsPtr)
	if len(nodeTabs) < demand {
		err = fmt.Errorf(GetAvailHostExcludeSpecifyErr+" zone:%v  err:%v ,ActiveNodeCount:%v  MatchNodeCount:%v  ",
			zone.Name, ClusterNotHaveAnyNodeToWrite, len(zone.DataNodes), len(nodeTabs))
		goto errDeal
	}

	nodeTabs.SetNodeTabCarry(availCarryCount, demand)
	sort.Sort(nodeTabs)

	for i := 0; i < demand; i++ {
		node := nodeTabs[i].Ptr
		node.SelectNodeForWrite()
		orderHosts = append(orderHosts, node.HttpAddr)
	}

	if newHosts, err = zone.DisOrderArray(&orderHosts); err != nil {
		err = fmt.Errorf(GetAvailHostExcludeSpecifyErr+"zone name:%v, err:%v  orderHosts is nil", zone.Name, err.Error())
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

func (zone *Zone) GetAvailCarryNodeTab(maxTotal uint64, specifyAddrsPtr *[]string) (nodeTabs NodeTabSorterByCarry, availCount int) {
	nodeTabs = make(NodeTabSorterByCarry, 0)
	for addr, dataNode := range zone.DataNodes {
		if AddrIsInSpecifyAddr(specifyAddrsPtr, addr) == true {
			continue
		}
		if dataNode.IsWriteAble() == false {
			continue
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
	}

	return
}

func (zone *Zone) DisOrderArray(oldHostsPtr *[]string) (newHosts []string, err error) {
	var (
		newCurrPos int
	)

	if oldHostsPtr == nil || *oldHostsPtr == nil || len(*oldHostsPtr) == 0 {
		log.LogError(fmt.Sprintf("action[DisOrderArray],zone:%v,err:%v", zone.Name, DisOrderArrayErr))
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
