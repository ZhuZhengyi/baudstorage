package datanode

import (
	"fmt"
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
}

func NewSpaceManager() (space *SpaceManager) {
	space = new(SpaceManager)
	space.disks = make(map[string]*Disk)
	space.vols = make(map[uint32]*Vol)
	go func() {
		ticker := time.Tick(time.Second * 10)
		for {
			select {
			case <-ticker:
				space.diskLock.RLocker()
				for _, d := range space.disks {
					d.recomputeVolCnt()
				}
				space.diskLock.RUnlock()
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
