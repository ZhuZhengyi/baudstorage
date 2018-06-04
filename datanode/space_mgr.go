package datanode

import (
	"fmt"
	"github.com/tiglabs/baudstorage/proto"
	"github.com/tiglabs/baudstorage/storage"
	"github.com/tiglabs/baudstorage/util/log"
	"math"
	"sync"
	"sync/atomic"
	"time"
)

type SpaceManager struct {
	disks    map[string]*Disk
	partions map[uint32]*DataPartion
	diskLock sync.RWMutex
	volLock  sync.RWMutex
	stats    *Stats
}

func NewSpaceManager(rack string) (space *SpaceManager) {
	space = new(SpaceManager)
	space.disks = make(map[string]*Disk)
	space.partions = make(map[uint32]*DataPartion)
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
	space.diskLock.RLock()
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
	space.diskLock.RLock()
	var (
		total, used, free                                    uint64
		createdPartionWeights, remainWeightsForCreatePartion uint64
		maxWeightsForCreatePartion, partionCnt               uint64
	)
	maxWeightsForCreatePartion = 0
	for _, d := range space.disks {
		d.recomputePartionCnt()
		total += d.All
		used += d.Used
		free += d.Free
		createdPartionWeights += d.CreatedPartionWeights
		remainWeightsForCreatePartion += d.RemainWeightsForCreatePartion
		partionCnt += d.PartionCnt
		if maxWeightsForCreatePartion < d.RemainWeightsForCreatePartion {
			maxWeightsForCreatePartion = d.RemainWeightsForCreatePartion
		}
	}
	space.diskLock.RUnlock()
	log.LogInfof("macheile total[%v] used[%v] free[%v]createdPartionWeights[%v]  remainWeightsForCreatePartion[%v]"+
		"partionCnt[%v]maxWeightsForCreatePartion[%v] ", total, used, free, createdPartionWeights, remainWeightsForCreatePartion, partionCnt, maxWeightsForCreatePartion)
	space.stats.updateMetrics(total, used, free, createdPartionWeights,
		remainWeightsForCreatePartion, maxWeightsForCreatePartion, partionCnt)
}

func (space *SpaceManager) getMinPartionCntDisk() (d *Disk) {
	space.diskLock.RLock()
	defer space.diskLock.RUnlock()
	var minVolCnt uint64
	minVolCnt = math.MaxUint64
	var path string
	for index, disk := range space.disks {
		if atomic.LoadUint64(&disk.PartionCnt) < minVolCnt {
			minVolCnt = atomic.LoadUint64(&disk.PartionCnt)
			path = index
		}
	}
	if path == "" {
		return nil
	}
	d = space.disks[path]

	return
}

func (space *SpaceManager) getDataPartion(partionId uint32) (dp *DataPartion) {
	space.volLock.RLock()
	defer space.volLock.RUnlock()
	v = space.partions[partionId]

	return
}

func (space *SpaceManager) putDataPartion(dp *DataPartion) {
	space.volLock.Lock()
	defer space.volLock.Unlock()
	space.partions[dp.partionId] = dp

	return
}

func (space *SpaceManager) chooseDiskAndCreateVol(partionId uint32, partionType string, storeSize int) (dp *DataPartion, err error) {
	if space.getDataPartion(partionId) != nil {
		return
	}
	d := space.getMinPartionCntDisk()
	if d == nil || d.Free < uint64(storeSize*2) {
		return nil, ErrNoDiskForCreateVol
	}
	dp, err = NewDataPartion(partionId, partionType, "", d.Path, storage.NewStoreMode, storeSize)
	if err == nil {
		space.putDataPartion(dp)
	}
	return
}

func (space *SpaceManager) deleteVol(vodId uint32) {
	dp := space.getDataPartion(vodId)
	if v == nil {
		return
	}
	space.volLock.Lock()
	delete(space.partions, vodId)
	space.volLock.Unlock()
	dp.exitCh <- true
	switch dp.partionType {
	case proto.ExtentVol:
		store := dp.store.(*storage.ExtentStore)
		store.ClostAll()

	case proto.TinyVol:
		store := dp.store.(*storage.TinyStore)
		store.CloseAll()
	}
}

func (s *DataNode) fillHeartBeatResponse(response *proto.DataNodeHeartBeatResponse) {
	response.Status = proto.TaskSuccess
	stat := s.space.stats
	stat.Lock()
	response.Used = stat.Used
	response.Total = stat.Total
	response.Free = stat.Free
	response.CreatedVolCnt = uint32(stat.CreatedPartionCnt)
	response.CreatedVolWeights = stat.CreatedPartionWeights
	response.MaxWeightsForCreateVol = stat.MaxWeightsForCreatePartion
	response.RemainWeightsForCreateVol = stat.RemainWeightsForCreatePartion
	stat.Unlock()

	response.RackName = s.rackName
	response.PartionInfo = make([]*proto.PartionReport, 0)
	space := s.space
	space.volLock.RLock()
	for _, dp := range space.partions {
		vr := &proto.PartionReport{PartionID: uint64(dp.partionId), PartionStatus: dp.status, Total: uint64(dp.partionSize), Used: uint64(dp.used)}
		response.PartionInfo = append(response.PartionInfo, vr)
	}
	space.volLock.RUnlock()
}

func (space *SpaceManager) modifyVolsStatus() {
	space.diskLock.RLock()
	defer space.diskLock.RUnlock()
	for _, d := range space.disks {
		partions := d.getDataPartions()
		diskStatus := d.Status

		for _, pid := range partions {
			dp := space.getDataPartion(pid)
			if dp == nil {
				continue
			}

			switch dp.partionType {
			case proto.ExtentVol:
				store := dp.store.(*storage.ExtentStore)
				dp.status = store.GetStoreStatus()
				dp.used = int(store.GetStoreUsedSize())
			case proto.TinyVol:
				store := dp.store.(*storage.TinyStore)
				dp.status = store.GetStoreStatus()
				if dp.isLeader {
					store.MoveChunkToUnavailChan()
				}
				dp.used = int(store.GetStoreUsedSize())
			}
			if dp.isLeader && dp.status == storage.ReadOnlyStore {
				dp.status = storage.ReadOnlyStore
			}

			if diskStatus == storage.DiskErrStore {
				dp.status = storage.DiskErrStore
			}
		}
	}
}
