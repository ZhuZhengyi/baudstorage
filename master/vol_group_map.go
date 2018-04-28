package master

import (
	"encoding/json"
	"fmt"
	"github.com/tiglabs/baudstorage/util/log"
	"runtime"
	"sync"
	"time"
)

type VolGroupMap struct {
	clusterID   string
	clusterType string
	sync.RWMutex
	volGroupMap        map[uint64]*VolGroup
	volGroupCount      int
	readWriteVolGroups int
	lastCheckVolID     uint64
	lastReleaseVolID   uint64
	volGroups          []*VolGroup
	cacheVolResponse   []byte
}

func NewVolMap() (vm *VolGroupMap) {
	vm = new(VolGroupMap)
	vm.volGroupMap = make(map[uint64]*VolGroup, 0)
	vm.volGroupCount = 1
	vm.volGroups = make([]*VolGroup, 0)

	return
}

func (vm *VolGroupMap) getVol(volID uint64) (*VolGroup, error) {
	vm.RLock()
	defer vm.RUnlock()
	if v, ok := vm.volGroupMap[volID]; ok {
		return v, nil
	}
	log.LogError(fmt.Sprintf("action[getVol],VolID:%v,err:%v", volID, VolNotFound))
	return nil, VolNotFound
}

func (vm *VolGroupMap) putVol(v *VolGroup) {
	vm.Lock()
	vm.volGroupMap[v.VolID] = v
	vm.volGroups = append(vm.volGroups, v)
	vm.Unlock()
}

func (vm *VolGroupMap) updateVolResponseCache(needUpdate bool, minVolID uint64) (body []byte, err error) {
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

func (vm *VolGroupMap) GetVolsView(minVolID uint64) (vrs []*VolResponse) {
	vrs = make([]*VolResponse, 0)
	for _, vol := range vm.volGroupMap {
		if vol.VolID <= minVolID {
			continue
		}
		if vol.status == VolUnavailable {
			continue
		}
		vr := vol.convertToVolResponse()
		vrs = append(vrs, vr)
	}

	return
}

func (vm *VolGroupMap) dataNodeOffline(nodeAddr string) {

}

func (vm *VolGroupMap) getNeedReleaseVolGroups(everyReleaseVolCount int, releaseVolAfterLoadVolSeconds int64) (needReleaseVolGroups []*VolGroup) {
	needReleaseVolGroups = make([]*VolGroup, 0)
	vm.RLock()
	defer vm.RUnlock()

	for i := 0; i < everyReleaseVolCount; i++ {
		if vm.lastReleaseVolID > (uint64)(len(vm.volGroupMap)) {
			vm.lastReleaseVolID = 0
		}
		vm.lastReleaseVolID++
		vg, ok := vm.volGroupMap[vm.lastReleaseVolID]
		if ok && time.Now().Unix()-vg.LastLoadTime >= releaseVolAfterLoadVolSeconds {
			needReleaseVolGroups = append(needReleaseVolGroups, vg)
		}
	}

	return
}

func (vm *VolGroupMap) releaseVolGroups(needReleaseVols []*VolGroup) {
	defer func() {
		if err := recover(); err != nil {
			const size = RuntimeStackBufSize
			buf := make([]byte, size)
			buf = buf[:runtime.Stack(buf, false)]
			log.LogError(fmt.Sprintf("releaseVolGroups panic %v: %s\n", err, buf))
		}
	}()
	var wg sync.WaitGroup
	for _, vg := range needReleaseVols {
		wg.Add(1)
		go func(vg *VolGroup) {
			vg.ReleaseVol()
			wg.Done()
		}(vg)
	}
	wg.Wait()

}

func (vm *VolGroupMap) getNeedCheckVolGroups(everyLoadVolCount int, loadVolFrequencyTime int64) (needCheckVols []*VolGroup) {
	needCheckVols = make([]*VolGroup, 0)
	vm.RLock()
	defer vm.RUnlock()

	for i := 0; i < everyLoadVolCount; i++ {
		if vm.lastCheckVolID > (uint64)(len(vm.volGroupMap)) {
			vm.lastCheckVolID = 0
		}
		vm.lastCheckVolID++
		v, ok := vm.volGroupMap[vm.lastCheckVolID]
		if ok && time.Now().Unix()-v.LastLoadTime >= loadVolFrequencyTime {
			needCheckVols = append(needCheckVols, v)
		}
	}

	return
}
