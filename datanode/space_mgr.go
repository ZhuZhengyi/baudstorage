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
	disks             map[string]*Disk
	partitions        map[uint32]*DataPartition
	diskLock          sync.RWMutex
	dataPartitionLock sync.RWMutex
	stats             *Stats
}

func NewSpaceManager(rack string) (space *SpaceManager) {
	space = new(SpaceManager)
	space.disks = make(map[string]*Disk)
	space.partitions = make(map[uint32]*DataPartition)
	space.stats = NewStats(rack)
	go func() {
		ticker := time.Tick(time.Second * 10)
		for {
			select {
			case <-ticker:
				space.modifyVolsStatus()
				space.updateMetrics()
				space.closeActiveFiles()
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
		return nil, fmt.Errorf("disk[%v] not exsit", path)
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
		total, used, free                                        uint64
		createdPartitionWeights, remainWeightsForCreatePartition uint64
		maxWeightsForCreatePartition, partitionCnt               uint64
	)
	maxWeightsForCreatePartition = 0
	for _, d := range space.disks {
		d.recomputePartitionCnt()
		total += d.All
		used += d.Used
		free += d.Free
		createdPartitionWeights += d.CreatedPartitionWeights
		remainWeightsForCreatePartition += d.RemainWeightsForCreatePartition
		partitionCnt += d.PartitionCnt
		if maxWeightsForCreatePartition < d.RemainWeightsForCreatePartition {
			maxWeightsForCreatePartition = d.RemainWeightsForCreatePartition
		}
	}
	space.diskLock.RUnlock()
	log.LogInfof("machine total[%v] used[%v] free[%v]createdPartitionWeights[%v]  remainWeightsForCreatePartition[%v]"+
		"partitionCnt[%v]maxWeightsForCreatePartition[%v] ", total, used, free, createdPartitionWeights, remainWeightsForCreatePartition, partitionCnt, maxWeightsForCreatePartition)
	space.stats.updateMetrics(total, used, free, createdPartitionWeights,
		remainWeightsForCreatePartition, maxWeightsForCreatePartition, partitionCnt)
}

func (space *SpaceManager) getMinPartitionCntDisk() (d *Disk) {
	space.diskLock.Lock()
	defer space.diskLock.Unlock()
	var minVolCnt uint64
	minVolCnt = math.MaxUint64
	var path string
	for index, disk := range space.disks {
		if atomic.LoadUint64(&disk.PartitionCnt) < minVolCnt {
			minVolCnt = atomic.LoadUint64(&disk.PartitionCnt)
			path = index
		}
	}
	if path == "" {
		return nil
	}
	d = space.disks[path]

	return
}

func (space *SpaceManager) getDataPartition(partitionId uint32) (dp *DataPartition) {
	space.dataPartitionLock.RLock()
	defer space.dataPartitionLock.RUnlock()
	dp = space.partitions[partitionId]

	return
}

func (space *SpaceManager) putDataPartition(dp *DataPartition) {
	space.dataPartitionLock.Lock()
	defer space.dataPartitionLock.Unlock()
	space.partitions[dp.partitionId] = dp

	return
}

func (space *SpaceManager) chooseDiskAndCreateVol(partitionId uint32, partitionType string, storeSize int) (dp *DataPartition, err error) {
	if space.getDataPartition(partitionId) != nil {
		return
	}
	d := space.getMinPartitionCntDisk()
	if d == nil || d.Free < uint64(storeSize*2) {
		return nil, ErrNoDiskForCreateVol
	}
	dp, err = NewDataPartition(partitionId, partitionType, "", d.Path, storage.NewStoreMode, storeSize)
	if err == nil {
		space.putDataPartition(dp)
	}
	return
}

func (space *SpaceManager) deleteVol(vodId uint32) {
	dp := space.getDataPartition(vodId)
	if dp == nil {
		return
	}
	space.dataPartitionLock.Lock()
	delete(space.partitions, vodId)
	space.dataPartitionLock.Unlock()
	dp.exitCh <- true
	switch dp.partitionType {
	case proto.ExtentVol:
		store := dp.store.(*storage.ExtentStore)
		store.ClostAll()

	case proto.TinyVol:
		store := dp.store.(*storage.TinyStore)
		store.CloseAll()
	}
}

func (space *SpaceManager)closeActiveFiles(){
	partitions:=make([]*DataPartition,0)
	space.dataPartitionLock.RLock()
	for _,partition:=range space.partitions{
		partitions=append(partitions,partition)
	}
	space.dataPartitionLock.RUnlock()
	activeFiles:=0
	for _,partition:=range partitions{
		if partition.partitionType==proto.ExtentVol{
			store:=partition.store.(*storage.ExtentStore)
			activeFiles+=store.GetStoreActiveFiles()
		}
	}
	if activeFiles>=MaxActiveExtents{
		for _,partition:=range partitions{
			if partition.partitionType==proto.ExtentVol{
				store:=partition.store.(*storage.ExtentStore)
				store.CloseStoreActiveFiles()
			}
		}
	}
}

func (s *DataNode) fillHeartBeatResponse(response *proto.DataNodeHeartBeatResponse) {
	response.Status = proto.TaskSuccess
	stat := s.space.stats
	stat.Lock()
	response.Used = stat.Used
	response.Total = stat.Total
	response.Free = stat.Free
	response.CreatedVolCnt = uint32(stat.CreatedPartitionCnt)
	response.CreatedVolWeights = stat.CreatedPartitionWeights
	response.MaxWeightsForCreateVol = stat.MaxWeightsForCreatePartition
	response.RemainWeightsForCreateVol = stat.RemainWeightsForCreatePartition
	stat.Unlock()

	response.RackName = s.rackName
	response.PartitionInfo = make([]*proto.PartitionReport, 0)
	space := s.space
	space.dataPartitionLock.RLock()
	for _, dp := range space.partitions {
		vr := &proto.PartitionReport{PartitionID: uint64(dp.partitionId), PartitionStatus: dp.partitionStatus, Total: uint64(dp.partitionSize), Used: uint64(dp.used)}
		response.PartitionInfo = append(response.PartitionInfo, vr)
	}
	space.dataPartitionLock.RUnlock()
}

func (space *SpaceManager) modifyVolsStatus() {
	space.diskLock.RLock()
	defer space.diskLock.RUnlock()
	for _, d := range space.disks {
		partitions := d.getDataPartitions()
		diskStatus := d.Status

		for _, pid := range partitions {
			dp := space.getDataPartition(pid)
			if dp == nil {
				continue
			}

			switch dp.partitionType {
			case proto.ExtentVol:
				store := dp.store.(*storage.ExtentStore)
				dp.partitionStatus = store.GetStoreStatus()
				dp.used = int(store.GetStoreUsedSize())
			case proto.TinyVol:
				store := dp.store.(*storage.TinyStore)
				dp.partitionStatus = store.GetStoreStatus()
				if dp.isLeader {
					store.MoveChunkToUnavailChan()
				}
				dp.used = int(store.GetStoreUsedSize())
			}
			if dp.isLeader && dp.partitionStatus == storage.ReadOnlyStore {
				dp.partitionStatus = storage.ReadOnlyStore
			}

			if diskStatus == storage.DiskErrStore {
				dp.partitionStatus = storage.DiskErrStore
			}
		}
	}
}
