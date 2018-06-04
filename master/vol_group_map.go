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
	sync.RWMutex
	volGroupMap        map[uint64]*DataPartition
	volGroupCount      int
	readWriteVolGroups int
	lastCheckVolID     uint64
	lastReleaseVolID   uint64
	volGroups          []*DataPartition
	cacheVolResponse   []byte
}

func NewVolMap() (vm *VolGroupMap) {
	vm = new(VolGroupMap)
	vm.volGroupMap = make(map[uint64]*DataPartition, 0)
	vm.volGroupCount = 1
	vm.volGroups = make([]*DataPartition, 0)

	return
}

func (vm *VolGroupMap) getVol(volID uint64) (*DataPartition, error) {
	vm.RLock()
	defer vm.RUnlock()
	if v, ok := vm.volGroupMap[volID]; ok {
		return v, nil
	}
	log.LogError(fmt.Sprintf("action[getVol],VolId:%v,err:%v", volID, DataPartitionNotFound))
	return nil, DataPartitionNotFound
}

func (vm *VolGroupMap) putVol(v *DataPartition) {
	vm.Lock()
	defer vm.Unlock()
	vm.volGroupMap[v.PartitionID] = v
	vm.volGroups = append(vm.volGroups, v)
}

func (vm *VolGroupMap) updateVolResponseCache(needUpdate bool, minVolID uint64) (body []byte, err error) {
	vm.Lock()
	defer vm.Unlock()
	if vm.cacheVolResponse == nil || needUpdate || len(vm.cacheVolResponse) == 0 {
		vm.cacheVolResponse = make([]byte, 0)
		vrs := vm.GetVolsView(minVolID)
		if len(vrs) == 0 {
			log.LogError(fmt.Sprintf("action[updateVolResponseCache],minVolID:%v,err:%v",
				minVolID, NoAvailDataPartition))
			return nil, NoAvailDataPartition
		}
		cv := NewVolsView()
		cv.Vols = vrs
		if body, err = json.Marshal(cv); err != nil {
			log.LogError(fmt.Sprintf("action[updateVolResponseCache],minVolID:%v,err:%v",
				minVolID, err.Error()))
			return nil, fmt.Errorf("%v,marshal err:%v", NoAvailDataPartition, err.Error())
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
	log.LogDebugf("volGroupMapLen[%v],volGroupsLen[%v],minVolID[%v],volGroupMap[%v],volGroups[%v]", len(vm.volGroupMap), len(vm.volGroups), minVolID, vm.volGroupMap, vm.volGroups)
	for _, vol := range vm.volGroupMap {
		if vol.PartitionID <= minVolID {
			continue
		}
		vr := vol.convertToVolResponse()
		vrs = append(vrs, vr)
	}

	return
}

func (vm *VolGroupMap) getNeedReleaseVolGroups(everyReleaseVolCount int, releaseVolAfterLoadVolSeconds int64) (needReleaseVolGroups []*DataPartition) {
	needReleaseVolGroups = make([]*DataPartition, 0)
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

func (vm *VolGroupMap) releaseVolGroups(needReleaseVols []*DataPartition) {
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
		go func(vg *DataPartition) {
			vg.ReleaseVol()
			wg.Done()
		}(vg)
	}
	wg.Wait()

}

func (vm *VolGroupMap) getNeedCheckVolGroups(everyLoadVolCount int, loadVolFrequencyTime int64) (needCheckVols []*DataPartition) {
	needCheckVols = make([]*DataPartition, 0)
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
