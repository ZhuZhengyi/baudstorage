package vol

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

const (
	MinWritableVolNum = 20
)

type VolGroup struct {
	VolID      uint32
	Status     uint8
	ReplicaNum uint8
	Hosts      []string
}

type VolsView struct {
	Vols []*VolGroup
}

func (vg *VolGroup) GetAllAddrs() (m string) {
	return strings.Join(vg.Hosts[1:], proto.AddrSplit) + proto.AddrSplit
}

type VolGroupWrapper struct {
	sync.RWMutex
	namespace   string
	master      []string
	conns       *pool.ConnPool
	volGroups   map[uint32]*VolGroup
	rwVolGroups []*VolGroup
}

func NewVolGroupWraper(namespace, masterHosts string) (vw *VolGroupWrapper, err error) {
	master := strings.Split(masterHosts, ",")
	vw = new(VolGroupWrapper)
	vw.master = master
	vw.namespace = namespace
	vw.conns = pool.NewConnPool()
	vw.rwVolGroups = make([]*VolGroup, 0)
	vw.volGroups = make(map[uint32]*VolGroup)
	if err = vw.getVolsFromMaster(); err != nil {
		return
	}
	go vw.update()
	return
}

func (vw *VolGroupWrapper) update() {
	ticker := time.NewTicker(time.Minute * 5)
	for {
		select {
		case <-ticker.C:
			vw.getVolsFromMaster()
		}
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
		log.LogInfof("Get VolView from master: %v", string(body))
		vw.updateVolGroup(views.Vols)
		break
	}

	return
}

func (vw *VolGroupWrapper) replaceOrInsertVol(vg *VolGroup) {
	vw.Lock()
	defer vw.Unlock()
	if _, ok := vw.volGroups[vg.VolID]; ok {
		delete(vw.volGroups, vg.VolID)
	}
	vw.volGroups[vg.VolID] = vg
}

func (vw *VolGroupWrapper) updateVolGroup(vols []*VolGroup) {
	rwVolGroups := make([]*VolGroup, 0)
	for _, vg := range vols {
		if vg.Status == storage.ReadWriteStore {
			rwVolGroups = append(rwVolGroups, vg)
		}
	}

	// If the view received from master cannot guarentee the minimum number
	// of volume groups, it is probably due to a **temporary** network problem
	// between master and datanode. So do not update the vol group view for
	// now, and use the old information.
	if len(rwVolGroups) < MinWritableVolNum {
		return
	}
	vw.rwVolGroups = rwVolGroups

	for _, vg := range vols {
		vw.replaceOrInsertVol(vg)
	}
}

func isExcluded(volID uint32, excludes []uint32) bool {
	for _, id := range excludes {
		if id == volID {
			return true
		}
	}
	return false
}

func (vw *VolGroupWrapper) GetWriteVol(exclude []uint32) (*VolGroup, error) {
	rwVolGroups := vw.rwVolGroups
	if len(rwVolGroups) == 0 {
		return nil, fmt.Errorf("No writable VolGroup")
	}

	rand.Seed(time.Now().UnixNano())
	choose := rand.Intn(len(rwVolGroups))
	vg := rwVolGroups[choose]
	if !isExcluded(vg.VolID, exclude) {
		return vg, nil
	}

	for _, vg = range rwVolGroups {
		if !isExcluded(vg.VolID, exclude) {
			return vg, nil
		}
	}
	return nil, fmt.Errorf("No writable VolGroup")
}

func (vw *VolGroupWrapper) GetVol(volID uint32) (*VolGroup, error) {
	vw.RLock()
	defer vw.RUnlock()
	vg, ok := vw.volGroups[volID]
	if !ok {
		return nil, fmt.Errorf("volGroup[%v] not exsit", volID)
	}
	return vg, nil
}

func (vw *VolGroupWrapper) GetConnect(addr string) (net.Conn, error) {
	return vw.conns.Get(addr)
}

func (vw *VolGroupWrapper) PutConnect(conn net.Conn) {
	vw.conns.Put(conn)
}
