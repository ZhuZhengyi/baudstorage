package datanode

import (
	"fmt"
	"github.com/tiglabs/baudstorage/proto"
	"github.com/tiglabs/baudstorage/storage"
	"math"
	"sync"
	"sync/atomic"
	"time"
)

type SpaceManager struct {
	disks    map[string]*Disk
	vols     map[uint32]*Vol
	diskLock sync.RWMutex
	volLock  sync.RWMutex
	stats    *Stats
}

func NewSpaceManager(rack string) (space *SpaceManager) {
	space = new(SpaceManager)
	space.disks = make(map[string]*Disk)
	space.vols = make(map[uint32]*Vol)
	space.stats = NewStats(rack)
	go func() {
		ticker := time.Tick(time.Second * 10)
		for {
			select {
			case <-ticker:
				space.modifyVolsStatus()
				space.updateMetrics()
			}
		}
	}()

	return
}

func (space *SpaceManager) getDisk(path string) (d *Disk, err error) {
	space.diskLock.RLocker()
	defer space.diskLock.RUnlock()
	d = space.disks[path]
	if d == nil {
		return nil, fmt.Errorf("Disk[%v] not exsit", path)
	}
	return
}

func (space *SpaceManager) putDisk(d *Disk) {
	space.diskLock.Lock()
	space.disks[d.Path] = d
	space.diskLock.Unlock()

}

func (space *SpaceManager) updateMetrics() {
	space.diskLock.RLocker()
	var (
		total, used, free                            uint64
		createdVolWeights, remainWeightsForCreateVol uint64
		maxWeightsForCreateVol, volcnt               uint64
	)
	maxWeightsForCreateVol = 0
	for _, d := range space.disks {
		d.recomputeVolCnt()
		total += d.All
		used += d.Used
		free += d.Free
		createdVolWeights += d.CreatedVolWeights
		remainWeightsForCreateVol += d.RemainWeightsForCreateVol
		volcnt += d.VolCnt
		if maxWeightsForCreateVol > d.RemainWeightsForCreateVol {
			maxWeightsForCreateVol = d.RemainWeightsForCreateVol
		}
	}
	space.diskLock.RUnlock()
	space.stats.updateMetrics(total, used, free, createdVolWeights,
		remainWeightsForCreateVol, maxWeightsForCreateVol, volcnt)
}

func (space *SpaceManager) getMinVolCntDisk() (d *Disk) {
	space.diskLock.RLocker()
	defer space.diskLock.RUnlock()
	var minVolCnt uint64
	minVolCnt = math.MaxUint64
	var path string
	for index, disk := range space.disks {
		if atomic.LoadUint64(&disk.VolCnt) < minVolCnt {
			minVolCnt = atomic.LoadUint64(&disk.VolCnt)
			path = index
		}
	}
	if path == "" {
		return nil
	}
	d = space.disks[path]

	return
}

func (space *SpaceManager) getVol(volId uint32) (v *Vol) {
	space.volLock.RLocker()
	defer space.volLock.RUnlock()
	v = space.vols[volId]

	return
}

func (space *SpaceManager) putVol(v *Vol) {
	space.volLock.Lock()
	defer space.volLock.Unlock()
	space.vols[v.volId] = v

	return
}

func (space *SpaceManager) chooseDiskAndCreateVol(volId uint32, volMode string, storeSize int) (v *Vol, err error) {
	if space.getVol(volId) != nil {
		return
	}
	d := space.getMinVolCntDisk()
	if d == nil || d.Free < uint64(storeSize*2) {
		return nil, ErrNoDiskForCreateVol
	}
	v, err = NewVol(volId, volMode, "", d.Path, storage.NewStoreMode, storeSize)
	if err == nil {
		space.putVol(v)
	}
	return
}

func (space *SpaceManager) deleteVol(vodId uint32) {
	v := space.getVol(vodId)
	if v == nil {
		return
	}
	space.volLock.Lock()
	delete(space.vols, vodId)
	space.volLock.Unlock()
	v.exitCh <- true
	switch v.volMode {
	case ExtentVol:
		store := v.store.(*storage.ExtentStore)
		store.ClostAll()

	case TinyVol:
		store := v.store.(*storage.TinyStore)
		store.CloseAll()
	}
}

func (s *DataNode) fillHeartBeatResponse(response *proto.DataNodeHeartBeatResponse) {
	response.Status = proto.OpOk
	stat := s.space.stats
	stat.Lock()
	response.Used = stat.Used
	response.Total = stat.Total
	response.Free = stat.Free
	response.CreatedVolCnt = uint32(stat.CreatedVolCnt)
	response.CreatedVolWeights = stat.CreatedVolWeights
	response.MaxWeightsForCreateVol = stat.MaxWeightsForCreateVol
	response.RemainWeightsForCreateVol = stat.RemainWeightsForCreateVol
	stat.Unlock()

	response.RackName = s.rackName
	response.VolInfo = make([]*proto.VolReport, 0)
	space := s.space
	space.volLock.RLock()
	for _, v := range space.vols {
		vr := &proto.VolReport{VolID: uint64(v.volId), VolStatus: v.status, Total: uint64(v.volSize), Used: uint64(v.used)}
		response.VolInfo = append(response.VolInfo, vr)
	}
	space.volLock.RUnlock()
}

func (space *SpaceManager) modifyVolsStatus() {
	space.diskLock.RLock()
	defer space.diskLock.RUnlock()
	for _, d := range space.disks {
		volsID := d.getVols()
		diskStatus := d.Status

		for _, vID := range volsID {
			v := space.getVol(vID)
			if v == nil {
				continue
			}

			switch v.volMode {
			case ExtentVol:
				store := v.store.(*storage.ExtentStore)
				v.status = store.GetStoreStatus()
				v.used = int(store.GetStoreUsedSize())
			case TinyVol:
				store := v.store.(*storage.TinyStore)
				v.status = store.GetStoreStatus()
				if v.isLeader {
					store.MoveChunkToUnavailChan()
				}
				v.used = int(store.GetStoreUsedSize())
			}
			if v.isLeader && v.status == storage.ReadOnlyStore {
				v.status = storage.ReadOnlyStore
			}

			if diskStatus == storage.DiskErrStore {
				v.status = storage.DiskErrStore
			}
		}
	}
}
