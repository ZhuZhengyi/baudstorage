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

type VolGroupWraper struct {
	MasterAddrs   []string
	volGroups     map[uint32]*VolGroup
	readWriteVols []*VolGroup
	ConnPool      *pool.ConnPool
	sync.RWMutex
}

func NewVolGroupWraper(masterHosts string) (wraper *VolGroupWraper, err error) {
	master := strings.Split(masterHosts, ",")
	wraper = new(VolGroupWraper)
	wraper.MasterAddrs = master
	wraper.ConnPool = pool.NewConnPool()
	wraper.readWriteVols = make([]*VolGroup, 0)
	wraper.volGroups = make(map[uint32]*VolGroup)
	if err = wraper.getVolsFromMaster(); err != nil {
		return
	}
	go wraper.update()
	return
}

func (wraper *VolGroupWraper) update() {
	wraper.getVolsFromMaster()
	ticker := time.NewTicker(time.Minute * 5)
	for {
		select {
		case <-ticker.C:
			wraper.getVolsFromMaster()
		}
	}
}

func (wraper *VolGroupWraper) getVolsFromMaster() (err error) {
	for _, m := range wraper.MasterAddrs {
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
		wraper.updateVolGroup(views)
		break
	}

	return
}

func (wraper *VolGroupWraper) insertVol(vg VolGroup) {
	wraper.RLock()
	volGroup := wraper.volGroups[vg.VolId]
	wraper.RUnlock()
	if volGroup == nil {
		wraper.Lock()
		wraper.volGroups[vg.VolId] = &VolGroup{VolId: vg.VolId, Status: vg.Status, Hosts: vg.Hosts, Goal: vg.Goal}
		wraper.Unlock()
	} else {
		volGroup.Status = vg.Status
		volGroup.Hosts = vg.Hosts
		volGroup.Goal = vg.Goal
	}
}

func (wraper *VolGroupWraper) updateVolGroup(views []VolGroup) {
	wraper.RLock()
	if len(views) < len(wraper.volGroups) {
		wraper.RUnlock()
		return
	}
	wraper.RUnlock()
	for _, vg := range views {
		wraper.insertVol(vg)
	}
	wraper.Lock()
	readWriteVols := make([]*VolGroup, 0)
	for _, vg := range wraper.volGroups {
		if vg.Status == storage.ReadWriteStore {
			readWriteVols = append(readWriteVols, vg)
		}
	}
	if len(readWriteVols) > 20 {
		wraper.readWriteVols = readWriteVols
	}
	wraper.Unlock()

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

func (wraper *VolGroupWraper) GetWriteVol(exclude []uint32) (v *VolGroup, err error) {
	wraper.RLock()
	rand.Seed(time.Now().UnixNano())
	randomIndex := rand.Intn(len(wraper.readWriteVols))
	v = wraper.readWriteVols[randomIndex]
	wraper.RUnlock()
	if !isExcluse(v.VolId, &exclude) {
		return
	}
	wraper.RLock()
	defer wraper.RUnlock()
	for _, v = range wraper.readWriteVols {
		if !isExcluse(v.VolId, &exclude) {
			return
		}
	}

	return nil, fmt.Errorf("no volGroup for write")
}

func (wraper *VolGroupWraper) GetVol(volId uint32) (v *VolGroup, err error) {
	wraper.RLock()
	defer wraper.RUnlock()
	v = wraper.volGroups[volId]
	if v == nil {
		return v, fmt.Errorf("volGroup[%v] not exsit", volId)
	}
	return
}

func (wraper *VolGroupWraper) GetConnect(addr string) (conn net.Conn, err error) {
	return wraper.ConnPool.Get(addr)
}

func (wraper *VolGroupWraper) PutConnect(conn net.Conn) {
	wraper.ConnPool.Put(conn)

}
