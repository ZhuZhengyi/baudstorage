package datanode

import (
	"fmt"
	"github.com/juju/errors"
	"github.com/tiglabs/baudstorage/storage"
	"github.com/tiglabs/baudstorage/util"
	"github.com/tiglabs/baudstorage/util/log"
	"io"
	"io/ioutil"
	"path"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
)

type CompactTask struct {
	partitionId uint32
	chunkId     int
	isLeader    bool
}

func (t *CompactTask) toString() (m string) {
	return fmt.Sprintf("dataPartition[%v]_chunk[%v]_isLeader[%v]", t.partitionId, t.chunkId, t.isLeader)
}

const (
	CompactThreadNum = 4
)

var (
	ErrDiskCompactChanFull = errors.New("disk compact chan is full")
)

type Disk struct {
	sync.Mutex
	Path                            string
	ReadErrs                        uint64
	WriteErrs                       uint64
	All                             uint64
	Used                            uint64
	Free                            uint64
	PartitionCnt                    uint64
	RemainWeightsForCreatePartition uint64
	CreatedPartitionWeights         uint64
	MaxErrs                         int
	Status                          int
	PartitionNames                  []string
	RestSize                        uint64
	compactCh                       chan *CompactTask
	space                           *SpaceManager
}

func NewDisk(path string, restSize uint64, maxErrs int) (d *Disk) {
	d = new(Disk)
	d.Path = path
	d.RestSize = restSize
	d.MaxErrs = maxErrs
	d.PartitionNames = make([]string, 0)
	d.RestSize = util.GB * 1
	d.MaxErrs = 2000
	d.DiskUsage()
	d.compactCh = make(chan *CompactTask, CompactThreadNum)
	for i := 0; i < CompactThreadNum; i++ {
		go d.compact()
	}

	return
}

func (d *Disk) DiskUsage() (err error) {
	fs := syscall.Statfs_t{}
	err = syscall.Statfs(d.Path, &fs)
	if err != nil {
		return
	}
	d.All = fs.Blocks * uint64(fs.Bsize)
	d.Free = fs.Bfree*uint64(fs.Bsize) - d.RestSize
	d.Used = d.All - d.Free
	log.LogDebugf("action[Disk.DiskUsage] disk[%v] all[%v] free[%v] used[%v]", d.Path, d.All, d.Free, d.Used)

	return
}

func (d *Disk) addTask(t *CompactTask) (err error) {
	select {
	case d.compactCh <- t:
		return
	default:
		return errors.Annotatef(ErrDiskCompactChanFull, "diskPath:[%v] partitionId[%v]", d.Path, t.partitionId)
	}
}

func (d *Disk) addReadErr() {
	atomic.AddUint64(&d.ReadErrs, 1)
}

func (d *Disk) compact() {
	for {
		select {
		case t := <-d.compactCh:
			dp := d.space.getDataPartition(t.partitionId)
			if dp == nil {
				continue
			}
			err, release := dp.store.(*storage.TinyStore).DoCompactWork(t.chunkId)
			if err != nil {
				log.LogError("action[Disk.compact] task[%v] compact error[%v]", t.toString(), err.Error())
			} else {
				log.LogInfo("action[Disk.compact] task[%v] compact success Release [%v]", t.toString(), release)
			}
		}
	}
}

func (d *Disk) addWriteErr() {
	atomic.AddUint64(&d.WriteErrs, 1)
}

func (d *Disk) recomputePartitionCnt() {
	d.DiskUsage()
	finfos, err := ioutil.ReadDir(d.Path)
	if err != nil {
		return
	}
	var count uint64
	dataPartitionSize := 0
	dataPartitionnames := make([]string, 0)
	for _, finfo := range finfos {
		if finfo.IsDir() && strings.HasPrefix(finfo.Name(), DataPartitionPrefix) {
			arr := strings.Split(finfo.Name(), "_")
			if len(arr) != 4 {
				continue
			}
			_, dataPartitionSize, _, err = UnmarshDataPartitionName(finfo.Name())
			if err != nil {
				continue
			}
			count += 1
			dataPartitionnames = append(dataPartitionnames, finfo.Name())
		}
	}
	d.Lock()
	atomic.StoreUint64(&d.PartitionCnt, count)
	d.PartitionNames = dataPartitionnames
	d.RemainWeightsForCreatePartition = d.All - d.RestSize - uint64(len(d.PartitionNames)*dataPartitionSize)
	d.CreatedPartitionWeights = uint64(len(d.PartitionNames) * dataPartitionSize)
	d.Unlock()
}

func (d *Disk) UpdateSpaceInfo(localIp string) (err error) {
	var statsInfo syscall.Statfs_t
	if err = syscall.Statfs(d.Path, &statsInfo); err != nil {
		d.addReadErr()
	}

	currErrs := d.ReadErrs + d.WriteErrs
	if currErrs >= uint64(d.MaxErrs) {
		d.Status = storage.DiskErrStore
	} else if d.Free <= 0 {
		d.Status = storage.ReadOnlyStore
	} else {
		d.Status = storage.ReadWriteStore
	}
	msg := fmt.Sprintf("node[%v] Path[%v] total[%v] realAvail[%v] dataPartitionsAvail[%v]"+
		"MinRestSize[%v] maxErrs[%v] ReadErrs[%v] WriteErrs[%v] partitionStatus[%v]", localIp, d.Path,
		d.All, d.Free, d.RemainWeightsForCreatePartition, d.RestSize, d.MaxErrs, d.ReadErrs, d.WriteErrs, d.Status)
	log.LogInfo(msg)

	return
}

func (d *Disk) addVol(dp *DataPartition) {
	name := dp.toName()
	d.Lock()
	defer d.Unlock()
	d.PartitionNames = append(d.PartitionNames, name)
	atomic.AddUint64(&d.PartitionCnt, 1)
	d.RemainWeightsForCreatePartition = d.All - d.RestSize - uint64(len(d.PartitionNames)*dp.partitionSize)
	d.CreatedPartitionWeights += uint64(dp.partitionSize)
}

func (d *Disk) getDataPartitions() (partitionIds []uint32) {
	d.Lock()
	defer d.Unlock()
	partitionIds = make([]uint32, 0)
	for _, name := range d.PartitionNames {
		vid, _, _, err := UnmarshDataPartitionName(name)
		if err != nil {
			continue
		}
		partitionIds = append(partitionIds, vid)
	}
	return
}

func UnmarshDataPartitionName(name string) (partitionId uint32, partitionSize int, partitionMode string, err error) {
	arr := strings.Split(name, "_")
	if len(arr) != 4 {
		err = fmt.Errorf("error dataPartition name[%v]", name)
		return
	}
	var (
		pId int
	)
	if pId, err = strconv.Atoi(arr[2]); err != nil {
		return
	}
	if partitionSize, err = strconv.Atoi(arr[3]); err != nil {
		return
	}
	partitionId = uint32(pId)
	partitionMode = arr[1]
	return
}

func (d *Disk) loadDataPartition(space *SpaceManager) {
	d.Lock()
	defer d.Unlock()
	fileInfoList, err := ioutil.ReadDir(d.Path)
	if err != nil {
		log.LogErrorf("action[Disk.loadDataPartition] %dp.", err)
		return
	}
	for _, fileInfo := range fileInfoList {
		var dp *DataPartition
		partitionId, dataPartitionSize, partitionType, err := UnmarshDataPartitionName(fileInfo.Name())
		log.LogDebugf("acton[Disk.loadDataPartition] disk info path[%v] name[%v] partitionId[%v] partitionSize[%v] partitionType[%v] err[%v].",
			d.Path, fileInfo.Name(), partitionId, dataPartitionSize, partitionType, err)
		if err != nil {
			log.LogError(fmt.Sprintf("Load[%v] from Disk[%v] Err[%v] ", partitionId, d.Path, err.Error()))
			continue
		}
		dp, err = NewDataPartition(partitionId, partitionType, path.Join(d.Path, fileInfo.Name()), d.Path, storage.ReBootStoreMode, dataPartitionSize)
		if err != nil {
			log.LogError(fmt.Sprintf("Load[%v] from Disk[%v] Err[%v] ", partitionId, d.Path, err.Error()))
			continue
		}
		if space.getDataPartition(dp.partitionId) == nil {
			space.putDataPartition(dp)
		}

	}
}

func (s *DataNode) AddDiskErrs(partitionId uint32, err error, flag uint8) {
	if err == nil {
		return
	}
	dp := s.space.getDataPartition(partitionId)
	if dp == nil {
		return
	}
	d, _ := s.space.getDisk(dp.path)
	if d == nil || err == nil {
		return
	}
	if !IsDiskErr(err.Error()) {
		return
	}
	if flag == WriteFlag {
		d.addWriteErr()
	} else if flag == ReadFlag {
		d.addReadErr()
	}
}

func IsDiskErr(errMsg string) bool {
	if strings.Contains(errMsg, storage.ErrorUnmatchPara.Error()) || strings.Contains(errMsg, storage.ErrorChunkNotFound.Error()) ||
		strings.Contains(errMsg, storage.ErrorNoAvaliFile.Error()) || strings.Contains(errMsg, storage.ErrorObjNotFound.Error()) ||
		strings.Contains(errMsg, io.EOF.Error()) || strings.Contains(errMsg, storage.ErrSyscallNoSpace.Error()) ||
		strings.Contains(errMsg, storage.ErrorHasDelete.Error()) || strings.Contains(errMsg, ErrVolNotExist.Error()) ||
		strings.Contains(errMsg, storage.ErrObjectSmaller.Error()) ||
		strings.Contains(errMsg, storage.ErrPkgCrcUnmatch.Error()) || strings.Contains(errMsg, ErrStoreTypeMismatch.Error()) ||
		strings.Contains(errMsg, storage.ErrorNoUnAvaliFile.Error()) ||
		strings.Contains(errMsg, storage.ErrExtentNameFormat.Error()) || strings.Contains(errMsg, storage.ErrorAgain.Error()) ||
		strings.Contains(errMsg, ErrChunkOffsetMismatch.Error()) ||
		strings.Contains(errMsg, storage.ErrorCompaction.Error()) || strings.Contains(errMsg, storage.ErrorVolReadOnly.Error()) {
		return false
	}
	return true
}

func LoadFromDisk(path string, restSize uint64, maxErrs int, space *SpaceManager) (d *Disk, err error) {
	if d, err = space.getDisk(path); err != nil {
		d = NewDisk(path, restSize, maxErrs)
		d.loadDataPartition(space)
		space.putDisk(d)
		err = nil
	}
	return
}
