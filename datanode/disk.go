package datanode

import (
	"fmt"
	"github.com/juju/errors"
	"github.com/tiglabs/baudstorage/storage"
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
	volId    uint32
	chunkId  int
	isLeader bool
}

func (t *CompactTask) toString() (m string) {
	return fmt.Sprintf("vol[%v]_chunk[%v]_isLeader[%v]", t.volId, t.chunkId, t.isLeader)
}

const (
	CompactThreadNum = 4
)

var (
	ErrDiskCompactChanFull = errors.New("disk compact chan is full")
)

type Disk struct {
	Path     string
	ReadErr  uint64
	WriteErr uint64
	All      uint64
	Used     uint64
	Free     uint64
	VolCnt   uint64
	VolsName []string
	RestSize uint64
	sync.Mutex
	compactCh chan *CompactTask
	space     *SpaceManager
}

func NewDisk(path string) (d *Disk) {
	d = new(Disk)
	d.Path = path
	d.VolsName = make([]string, 0)
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

	return
}

func (d *Disk) addTask(t *CompactTask) (err error) {
	select {
	case d.compactCh <- t:
		return
	default:
		return errors.Annotatef(ErrDiskCompactChanFull, "diskPath:[%v] volId[%v]", d.Path, t.volId)
	}
}

func (d *Disk) addReadErr() {
	atomic.AddUint64(&d.ReadErr, 1)
}

func (d *Disk) compact() {
	for {
		select {
		case t := <-d.compactCh:
			v := d.space.getVol(t.volId)
			if v == nil {
				continue
			}
			err, release := v.store.(*storage.TinyStore).DoCompactWork(t.chunkId)
			if err != nil {
				log.LogError("Task[%v] compact error[%v]", t.toString(), err.Error())
			} else {
				log.LogInfo("Task[%v] compact success Release [%v]", t.toString(), release)
			}
		}
	}
}

func (d *Disk) addWriteErr() {
	atomic.AddUint64(&d.WriteErr, 1)
}

func (d *Disk) recomputeVolCnt() {
	finfos, err := ioutil.ReadDir(d.Path)
	if err != nil {
		return
	}
	var count uint64
	volnames := make([]string, 0)
	for _, finfo := range finfos {
		if finfo.IsDir() && strings.HasPrefix(finfo.Name(), VolPrefix) {
			arr := strings.Split(finfo.Name(), "_")
			if len(arr) != 4 {
				continue
			}
			count += 1
			volnames = append(volnames, finfo.Name())
		}
	}
	d.Lock()
	atomic.StoreUint64(&d.VolCnt, count)
	d.VolsName = volnames
	d.Unlock()
}

func (d *Disk) addVol(v *Vol) {
	name := v.toName()
	d.Lock()
	defer d.Unlock()
	d.VolsName = append(d.VolsName, name)
	atomic.AddUint64(&d.VolCnt, 1)
}

func (d *Disk) loadVol(space *SpaceManager) {
	d.Lock()
	defer d.Unlock()
	for _, name := range d.VolsName {
		arr := strings.Split(name, "_")
		var (
			vId, vSize int
			err        error
			v          *Vol
		)
		if vId, err = strconv.Atoi(arr[2]); err != nil {
			continue
		}
		if vSize, err = strconv.Atoi(arr[3]); err != nil {
			continue
		}
		v, err = NewVol(uint32(vId), arr[1], path.Join(d.Path, name), storage.ReBootStoreMode, vSize)
		if err != nil {
			log.LogError(fmt.Sprintf("LoadVol[%v] from Disk[%v] Err[%v] ", vId, d.Path, err.Error()))
			continue
		}
		space.putVol(v)
	}
}

func (s *DataNode) AddDiskErrs(volId uint32, err error, flag uint8) {
	v := s.space.getVol(volId)
	if v == nil {
		return
	}
	d, _ := s.space.getDisk(v.path)
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
		strings.Contains(errMsg, storage.ErrPkgCrcUnmatch.Error()) || strings.Contains(errMsg, ErrStoreTypeUnmatch.Error()) ||
		strings.Contains(errMsg, storage.ErrorNoUnAvaliFile.Error()) ||
		strings.Contains(errMsg, storage.ErrExtentNameFormat.Error()) || strings.Contains(errMsg, storage.ErrorAgain.Error()) ||
		strings.Contains(errMsg, ErrChunkOffsetUnmatch.Error()) ||
		strings.Contains(errMsg, storage.ErrorCompaction.Error()) || strings.Contains(errMsg, storage.ErrorVolReadOnly.Error()) {
		return false
	}
	return true
}

func LoadFromDisk(path string, space *SpaceManager) (d *Disk, err error) {
	if d, err = space.getDisk(path); err != nil {
		d = NewDisk(path)
		d.loadVol(space)
		space.putDisk(d)
	}

	return
}
