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
	partionId uint32
	chunkId   int
	isLeader  bool
}

func (t *CompactTask) toString() (m string) {
	return fmt.Sprintf("vol[%v]_chunk[%v]_isLeader[%v]", t.partionId, t.chunkId, t.isLeader)
}

const (
	CompactThreadNum = 4
)

var (
	ErrDiskCompactChanFull = errors.New("disk compact chan is full")
)

type Disk struct {
	sync.Mutex
	Path                          string
	ReadErrs                      uint64
	WriteErrs                     uint64
	All                           uint64
	Used                          uint64
	Free                          uint64
	PartionCnt                    uint64
	RemainWeightsForCreatePartion uint64
	CreatedPartionWeights         uint64
	MaxErrs                       int
	Status                        int
	PartionNames                  []string
	RestSize                      uint64
	compactCh                     chan *CompactTask
	space                         *SpaceManager
}

func NewDisk(path string, restSize uint64, maxErrs int) (d *Disk) {
	d = new(Disk)
	d.Path = path
	d.RestSize = restSize
	d.MaxErrs = maxErrs
	d.PartionNames = make([]string, 0)
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
		return errors.Annotatef(ErrDiskCompactChanFull, "diskPath:[%v] partionId[%v]", d.Path, t.partionId)
	}
}

func (d *Disk) addReadErr() {
	atomic.AddUint64(&d.ReadErrs, 1)
}

func (d *Disk) compact() {
	for {
		select {
		case t := <-d.compactCh:
			v := d.space.getVol(t.partionId)
			if v == nil {
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

func (d *Disk) recomputePartionCnt() {
	d.DiskUsage()
	finfos, err := ioutil.ReadDir(d.Path)
	if err != nil {
		return
	}
	var count uint64
	dataPartionSize := 0
	volnames := make([]string, 0)
	for _, finfo := range finfos {
		if finfo.IsDir() && strings.HasPrefix(finfo.Name(), DataPartionPrefix) {
			arr := strings.Split(finfo.Name(), "_")
			if len(arr) != 4 {
				continue
			}
			_, dataPartionSize, _, err = UnmarshDataPartionName(finfo.Name())
			if err != nil {
				continue
			}
			count += 1
			volnames = append(volnames, finfo.Name())
		}
	}
	d.Lock()
	atomic.StoreUint64(&d.PartionCnt, count)
	d.PartionNames = volnames
	d.RemainWeightsForCreatePartion = d.All - d.RestSize - uint64(len(d.PartionNames)*dataPartionSize)
	d.CreatedPartionWeights = uint64(len(d.PartionNames) * dataPartionSize)
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
	msg := fmt.Sprintf("node[%v] Path[%v] total[%v] realAvail[%v] volsAvail[%v]"+
		"MinRestSize[%v] maxErrs[%v] ReadErrs[%v] WriteErrs[%v] status[%v]", localIp, d.Path,
		d.All, d.Free, d.RemainWeightsForCreatePartion, d.RestSize, d.MaxErrs, d.ReadErrs, d.WriteErrs, d.Status)
	log.LogInfo(msg)

	return
}

func (d *Disk) addVol(dp *DataPartion) {
	name := dp.toName()
	d.Lock()
	defer d.Unlock()
	d.PartionNames = append(d.PartionNames, name)
	atomic.AddUint64(&d.PartionCnt, 1)
	d.RemainWeightsForCreatePartion = d.All - d.RestSize - uint64(len(d.PartionNames)*dp.partionSize)
	d.CreatedPartionWeights += uint64(dp.partionSize)
}

func (d *Disk) getVols() (partionIds []uint32) {
	d.Lock()
	defer d.Unlock()
	partionIds = make([]uint32, 0)
	for _, name := range d.PartionNames {
		vid, _, _, err := UnmarshDataPartionName(name)
		if err != nil {
			continue
		}
		partionIds = append(partionIds, vid)
	}
	return
}

func UnmarshDataPartionName(name string) (partionId uint32, partionSize int, partionMode string, err error) {
	arr := strings.Split(name, "_")
	if len(arr) != 4 {
		err = fmt.Errorf("error vol name[%v]", name)
		return
	}
	var (
		pId int
	)
	if pId, err = strconv.Atoi(arr[2]); err != nil {
		return
	}
	if partionSize, err = strconv.Atoi(arr[3]); err != nil {
		return
	}
	partionId = uint32(pId)
	partionMode = arr[1]
	return
}

func (d *Disk) loadDataPartion(space *SpaceManager) {
	d.Lock()
	defer d.Unlock()
	fileInfoList, err := ioutil.ReadDir(d.Path)
	if err != nil {
		log.LogErrorf("action[Disk.loadDataPartion] %dp.", err)
		return
	}
	for _, fileInfo := range fileInfoList {
		var dp *DataPartion
		partionId, volSize, partionType, err := UnmarshDataPartionName(fileInfo.Name())
		log.LogDebugf("acton[Disk.loadDataPartion] disk info path[%v] name[%v] partionId[%v] partionSize[%v] partionType[%v] err[%v].", d.Path, fileInfo.Name(), partionId, volSize, partionType, err)
		if err != nil {
			log.LogError(fmt.Sprintf("LoadVol[%v] from Disk[%v] Err[%v] ", partionId, d.Path, err.Error()))
			continue
		}
		v, err = NewVol(partionId, partionType, path.Join(d.Path, fileInfo.Name()), d.Path, storage.ReBootStoreMode, volSize)
		if err != nil {
			log.LogError(fmt.Sprintf("LoadVol[%v] from Disk[%v] Err[%v] ", partionId, d.Path, err.Error()))
			continue
		}
		if space.getVol(dp.partionId) == nil {
			space.putVol(v)
		}

	}
}

func (s *DataNode) AddDiskErrs(partionId uint32, err error, flag uint8) {
	if err == nil {
		return
	}
	v := s.space.getVol(partionId)
	if v == nil {
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
		d.loadDataPartion(space)
		space.putDisk(d)
		err = nil
	}
	return
}
