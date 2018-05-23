package sdk

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"math/rand"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/tiglabs/baudstorage/proto"
	"github.com/tiglabs/baudstorage/storage"
	"github.com/tiglabs/baudstorage/util/log"
	"github.com/tiglabs/baudstorage/util/pool"
)

type VolGroup struct {
	VolId  uint32
	Goal   uint8
	Hosts  []string
	Status uint8
}

func (vg *VolGroup) GetAllAddrs() (m string) {
	for i, host := range vg.Hosts {
		if i == len(vg.Hosts)-1 {
			m = m + host
		} else {
			m = m + host + proto.AddrSplit
		}
	}
	return
}

const (
	VolViewUrl            = "/client/view"
	ActionGetVolGroupView = "ActionGetVolGroupView"
)

type VolGroupWrapper struct {
	MasterAddrs   []string
	volGroups     map[uint32]*VolGroup
	readWriteVols []*VolGroup
	ConnPool      *pool.ConnPool
	execludeVolCh chan uint32
	sync.RWMutex
}

func NewVolGroupWraper(masterHosts string) (wrapper *VolGroupWrapper, err error) {
	master := strings.Split(masterHosts, ",")
	wrapper = new(VolGroupWrapper)
	wrapper.MasterAddrs = master
	wrapper.ConnPool = pool.NewConnPool()
	wrapper.readWriteVols = make([]*VolGroup, 0)
	wrapper.volGroups = make(map[uint32]*VolGroup)
	wrapper.execludeVolCh = make(chan uint32, 10000)
	if err = wrapper.getVolsFromMaster(); err != nil {
		return
	}
	go wrapper.update()
	return
}

func (wrapper *VolGroupWrapper) update() {
	wrapper.getVolsFromMaster()
	ticker := time.NewTicker(time.Minute * 5)
	for {
		select {
		case <-ticker.C:
			wrapper.getVolsFromMaster()
		}
	}
}

func (wrapper *VolGroupWrapper) PutExcludeVol(volId uint32) {
	select {
	case wrapper.execludeVolCh <- volId:
		return
	default:
		return
	}
}

func (wrapper *VolGroupWrapper) getVolsFromMaster() (err error) {
	for _, m := range wrapper.MasterAddrs {
		if m == "" {
			continue
		}
		var resp *http.Response
		resp, err = http.Get("http://" + m + VolViewUrl)
		if err != nil {
			log.LogError(fmt.Sprintf(ActionGetVolGroupView+"get VolView from master[%v] err[%v]", m, err.Error()))
			continue
		}
		defer resp.Body.Close()
		body, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			continue
		}
		views := make([]VolGroup, 0)
		if err = json.Unmarshal(body, &views); err != nil {
			log.LogError(fmt.Sprintf(ActionGetVolGroupView+"get VolView from master[%v] err[%v]", m, err.Error()))
			continue
		}
		wrapper.updateVolGroup(views)
		break
	}

	return
}

func (wrapper *VolGroupWrapper) insertVol(vg VolGroup) {
	wrapper.RLock()
	volGroup := wrapper.volGroups[vg.VolId]
	wrapper.RUnlock()
	wrapper.Lock()
	if volGroup == nil {
		wrapper.volGroups[vg.VolId] = &VolGroup{VolId: vg.VolId, Status: vg.Status, Hosts: vg.Hosts, Goal: vg.Goal}
	} else {
		volGroup.Status = vg.Status
		volGroup.Hosts = vg.Hosts
		volGroup.Goal = vg.Goal
	}
	wrapper.Unlock()
}

func (wrapper *VolGroupWrapper) updateVolGroup(views []VolGroup) {
	wrapper.RLock()
	if len(views) < len(wrapper.volGroups) {
		wrapper.RUnlock()
		return
	}
	wrapper.RUnlock()
	for _, vg := range views {
		wrapper.insertVol(vg)
	}
	wrapper.Lock()
	readWriteVols := make([]*VolGroup, 0)
	for _, vg := range wrapper.volGroups {
		if vg.Status == storage.ReadWriteStore {
			readWriteVols = append(readWriteVols, vg)
		}
	}
	if len(readWriteVols) > 20 {
		wrapper.readWriteVols = readWriteVols
	}
	wrapper.Unlock()

	return
}

func isExcluse(volId uint32, excludes *[]uint32) (exclude bool) {
	for _, eId := range *excludes {
		if eId == volId {
			return true
		}
	}

	return
}

func (wrapper *VolGroupWrapper) GetWriteVol(exclude *[]uint32) (v *VolGroup, err error) {
	wrapper.RLock()
	rand.Seed(time.Now().UnixNano())
	randomIndex := rand.Intn(len(wrapper.readWriteVols))
	v = wrapper.readWriteVols[randomIndex]
	wrapper.RUnlock()
	if !isExcluse(v.VolId, exclude) {
		return
	}
	wrapper.RLock()
	defer wrapper.RUnlock()
	for _, v = range wrapper.readWriteVols {
		if !isExcluse(v.VolId, exclude) {
			return
		}
	}

	return nil, fmt.Errorf("no volGroup for write")
}

func (wrapper *VolGroupWrapper) GetVol(volId uint32) (v *VolGroup, err error) {
	wrapper.RLock()
	defer wrapper.RUnlock()
	v = wrapper.volGroups[volId]
	if v == nil {
		return v, fmt.Errorf("volGroup[%v] not exsit", volId)
	}
	return
}

func (wrapper *VolGroupWrapper) GetConnect(addr string) (conn net.Conn, err error) {
	return wrapper.ConnPool.Get(addr)
}

func (wrapper *VolGroupWrapper) PutConnect(conn net.Conn) {
	wrapper.ConnPool.Put(conn)

}
