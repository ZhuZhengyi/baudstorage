package master

import (
	"encoding/json"
	"fmt"
	"github.com/tiglabs/baudstorage/util/log"
	"sync"
)

type VolMap struct {
	clusterID   string
	clusterType string
	sync.RWMutex
	volMap           map[uint64]*VolGroup
	volCount         int
	readWriteVols    int
	lastCheckVolID   uint64
	lastReleaseVolID uint64
	vols             []*VolGroup
	cacheVolResponse []byte
}

func NewVolMap() (vm *VolMap) {
	vm = new(VolMap)
	vm.volMap = make(map[uint64]*VolGroup, 0)
	vm.volCount = 1
	vm.vols = make([]*VolGroup, 0)

	return
}

func (vm *VolMap) getVol(volID uint64) (*VolGroup, error) {
	vm.RLock()
	defer vm.RUnlock()
	if v, ok := vm.volMap[volID]; ok {
		return v, nil
	}
	log.LogError(fmt.Sprintf("action[getVol],VolID:%v,err:%v", volID, VolNotFound))
	return nil, VolNotFound
}

func (vm *VolMap) putVol(v *VolGroup) {
	vm.Lock()
	vm.volMap[v.VolID] = v
	vm.vols = append(vm.vols, v)
	vm.Unlock()
}

func (vm *VolMap) updateVolResponseCache(needUpdate bool, minVolID uint64) (body []byte, err error) {
	vm.Lock()
	defer vm.Unlock()
	if vm.cacheVolResponse == nil || needUpdate || len(vm.cacheVolResponse) == 0 {
		vm.cacheVolResponse = make([]byte, 0)
		vrs := vm.GetVolsView(minVolID)
		if len(vrs) == 0 {
			log.LogError(fmt.Sprintf("action[updateVolResponseCache],clusterID:%v,minVolID:%v,err:%v",
				vm.clusterID, minVolID, NoAvailVol))
			return nil, NoAvailVol
		}
		cv := NewVolsView()
		cv.Vols = vrs
		if body, err = json.Marshal(cv); err != nil {
			log.LogError(fmt.Sprintf("action[updateVolResponseCache],clusterID:%v,minVolID:%v,err:%v",
				vm.clusterID, minVolID, err.Error()))
			return nil, fmt.Errorf("%v,marshal err:%v", NoAvailVol, err.Error())
		}
		vm.cacheVolResponse = body
		return
	}
	body = make([]byte, len(vm.cacheVolResponse))
	copy(body, vm.cacheVolResponse)

	return
}

func (vm *VolMap) GetVolsView(minVolID uint64) (vrs []*VolResponse) {
	vrs = make([]*VolResponse, 0)
	for _, vol := range vm.volMap {
		if vol.VolID <= minVolID {
			continue
		}
		if vol.status == VolUnavailable {
			continue
		}
		vr := NewVolResponseForClient(vol)
		vrs = append(vrs, vr)
	}

	return
}

func NewVolResponseForClient(v *VolGroup) (vr *VolResponse) {
	vr = new(VolResponse)
	v.Lock()
	defer v.Unlock()
	vr.VolID = v.VolID
	vr.Status = v.status
	vr.ReplicaNum = v.replicaNum
	vr.Hosts = make([]string, len(v.PersistenceHosts))
	copy(vr.Hosts, v.PersistenceHosts)
	return
}

func (vm *VolMap) dataNodeOffline(nodeAddr string) {

}
