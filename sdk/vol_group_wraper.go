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

const (
	VolViewUrl            = "/client/vols?name="
	ActionGetVolGroupView = "ActionGetVolGroupView"
)

type VolGroup struct {
	VolId      uint32
	Status     uint8
	ReplicaNum uint8
	Hosts      []string
}

type VolsView struct {
	Vols []*VolGroup
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

type VolGroupWrapper struct {
	sync.RWMutex
	namespace     string
	master        []string
	volGroups     map[uint32]*VolGroup
	readWriteVols []*VolGroup
	conns         *pool.ConnPool
	execludeVolCh chan uint32
}

func NewVolGroupWraper(namespace, masterHosts string) (vw *VolGroupWrapper, err error) {
	master := strings.Split(masterHosts, ",")
	vw = new(VolGroupWrapper)
	vw.master = master
	vw.namespace = namespace
	vw.conns = pool.NewConnPool()
	vw.readWriteVols = make([]*VolGroup, 0)
	vw.volGroups = make(map[uint32]*VolGroup)
	vw.execludeVolCh = make(chan uint32, 10000)
	if err = vw.getVolsFromMaster(); err != nil {
		return
	}
	go vw.update()
	return
}

func (vw *VolGroupWrapper) update() {
	vw.getVolsFromMaster()
	ticker := time.NewTicker(time.Minute * 5)
	for {
		select {
		case <-ticker.C:
			vw.getVolsFromMaster()
		}
	}
}

func (vw *VolGroupWrapper) PutExcludeVol(volId uint32) {
	select {
	case vw.execludeVolCh <- volId:
		return
	default:
		return
	}
}

func (vw *VolGroupWrapper) getVolsFromMaster() (err error) {
	for _, m := range vw.master {
		if m == "" {
			continue
		}
		var resp *http.Response
		resp, err = http.Get("http://" + m + VolViewUrl + vw.namespace)
		if err != nil {
			log.LogError(fmt.Sprintf(ActionGetVolGroupView+"get VolView from master[%v] err[%v]", m, err.Error()))
			continue
		}
		defer resp.Body.Close()
		body, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			continue
		}
		views := new(VolsView)
		if err = json.Unmarshal(body, views); err != nil {
			log.LogError(fmt.Sprintf(ActionGetVolGroupView+"get VolView from master[%v] err[%v]", m, err.Error()))
			continue
		}
		vw.updateVolGroup(views.Vols)
		break
	}

	return
}

func (vw *VolGroupWrapper) insertVol(vg VolGroup) {
	vw.RLock()
	volGroup := vw.volGroups[vg.VolId]
	vw.RUnlock()
	vw.Lock()
	if volGroup == nil {
		vw.volGroups[vg.VolId] = &VolGroup{VolId: vg.VolId, Status: vg.Status, Hosts: vg.Hosts, ReplicaNum: vg.ReplicaNum}
	} else {
		volGroup.Status = vg.Status
		volGroup.Hosts = vg.Hosts
		volGroup.ReplicaNum = vg.ReplicaNum
	}
	vw.Unlock()
}

func (vw *VolGroupWrapper) updateVolGroup(views []*VolGroup) {
	vw.RLock()
	if len(views) < len(vw.volGroups) {
		vw.RUnlock()
		return
	}
	vw.RUnlock()
	for _, vg := range views {
		vw.insertVol(*vg)
	}
	vw.Lock()
	readWriteVols := make([]*VolGroup, 0)
	for _, vg := range vw.volGroups {
		if vg.Status == storage.ReadWriteStore {
			readWriteVols = append(readWriteVols, vg)
		}
	}
	if len(readWriteVols) > 20 {
		vw.readWriteVols = readWriteVols
	}
	vw.Unlock()

	return
}

func isExcluded(volID uint32, excludes []uint32) bool {
	for _, eid := range excludes {
		if eid == volID {
			return true
		}
	}
	return false
}

func (vw *VolGroupWrapper) GetWriteVol(exclude []uint32) (v *VolGroup, err error) {
	vw.RLock()
	if len(vw.readWriteVols) == 0 {
		vw.RUnlock()
		return nil, fmt.Errorf("no volGroup for write")
	}
	rand.Seed(time.Now().UnixNano())
	randomIndex := rand.Intn(len(vw.readWriteVols))
	v = vw.readWriteVols[randomIndex]
	vw.RUnlock()
	if !isExcluded(v.VolId, exclude) {
		return
	}
	vw.RLock()
	defer vw.RUnlock()
	for _, v = range vw.readWriteVols {
		if !isExcluded(v.VolId, exclude) {
			return
		}
	}

	return nil, fmt.Errorf("no volGroup for write")
}

func (vw *VolGroupWrapper) GetVol(volId uint32) (v *VolGroup, err error) {
	vw.RLock()
	defer vw.RUnlock()
	v = vw.volGroups[volId]
	if v == nil {
		return v, fmt.Errorf("volGroup[%v] not exsit", volId)
	}
	return
}

func (vw *VolGroupWrapper) GetConnect(addr string) (conn net.Conn, err error) {
	return vw.conns.Get(addr)
}

func (vw *VolGroupWrapper) PutConnect(conn net.Conn) {
	vw.conns.Put(conn)
}
