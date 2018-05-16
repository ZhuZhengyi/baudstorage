package storage

import (
	"container/list"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"io/ioutil"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"

	"github.com/juju/errors"
	"time"
)

var (
	ExtentOpenOpt = os.O_CREATE | os.O_RDWR | os.O_EXCL
)

const (
	BlockCrcHeaderSize = 4097
	BlockCount         = 1024
	MarkDelete         = 'D'
	UnMarkDelete       = 'U'
	MarkDeleteIndex    = 4096
	BlockSize          = 65536
	PerBlockCrcSize    = 4
)

type ExtentStore struct {
	dataDir      string
	lock         sync.Mutex
	extents      map[uint64]*Extent
	fdlist       *list.List
	baseExtentId uint64
	readErr      uint64
	writeErr     uint64
}

func NewExtentStore(dataDir string, storeSize int, newMode bool) (s *ExtentStore, err error) {
	s = new(ExtentStore)
	s.dataDir = dataDir
	if err = CheckAndCreateSubdir(dataDir, newMode); err != nil {
		return nil, fmt.Errorf("NewExtentStore [%v] err[%v]", dataDir, err)
	}

	s.extents = make(map[uint64]*Extent, 0)
	s.fdlist = list.New()
	if err = s.initBaseFileId(); err != nil {
		return nil, fmt.Errorf("NewExtentStore [%v] err[%v]", dataDir, err)
	}

	return
}

func (s *ExtentStore) DeleteStore() {
	s.ClearAllCache()
	os.RemoveAll(s.dataDir)

	return
}

func (s *ExtentStore) Create() (extentId uint64, err error) {
	var e *Extent
	fileId := atomic.AddUint64(&s.baseExtentId, 1)
	emptyCrc := crc32.ChecksumIEEE(make([]byte, BlockSize))
	if e, err = s.createExtent(fileId); err != nil {
		return
	}

	for blockNo := 0; blockNo < BlockCount; blockNo++ {
		binary.BigEndian.PutUint32(e.blocksCrc[blockNo*PerBlockCrcSize:(blockNo+1)*PerBlockCrcSize], emptyCrc)
	}

	if _, err = e.file.WriteAt(e.blocksCrc, 0); err != nil {
		return
	}
	if err = e.file.Sync(); err != nil {
		return
	}
	s.addExtentToCache(e)
	extentId = fileId

	return
}

func (s *ExtentStore) createExtent(extentId uint64) (e *Extent, err error) {
	name := s.dataDir + "/" + strconv.Itoa((int)(extentId))

	e = NewExtentInCore(name, extentId)
	if err = e.file.OpenFile(e.filePath, ExtentOpenOpt, 0666); err != nil {
		return nil, err
	}
	if err = os.Truncate(name, BlockCrcHeaderSize); err != nil {
		return nil, err
	}

	return
}

func (s *ExtentStore) getExtent(extentId uint64) (e *Extent, err error) {
	var ok bool
	if e, ok = s.getExtentFromCache(extentId); !ok {
		e, err = s.loadExtentFromDisk(extentId)
	}

	return e, err
}

func (s *ExtentStore) loadExtentFromDisk(extentId uint64) (e *Extent, err error) {
	name := s.dataDir + "/" + strconv.Itoa((int)(extentId))
	e = NewExtentInCore(name, extentId)
	if err = s.openExtentFromDisk(e); err == nil {
		s.addExtentToCache(e)
	}

	return
}

func (s *ExtentStore) initBaseFileId() error {
	var maxFileId int
	files, err := ioutil.ReadDir(s.dataDir)
	if err != nil {
		return err
	}

	for _, f := range files {
		extentId, err := strconv.Atoi(f.Name())
		if err != nil {
			continue
		}
		if extentId >= maxFileId {
			maxFileId = extentId
		}
	}
	s.baseExtentId = (uint64)(maxFileId)

	return nil
}

func (s *ExtentStore) openExtentFromDisk(e *Extent) (err error) {
	e.writelock()
	defer e.writeUnlock()

	if err = e.file.OpenFile(e.filePath, os.O_RDWR, 0666); err != nil {
		if strings.Contains(err.Error(), syscall.ENOENT.Error()) {
			err = ErrorChunkNotFound
		}
		return err
	}
	if _, err = e.file.ReadAt(e.blocksCrc, 0); err != nil {
		return
	}

	return
}

func (s *ExtentStore) Write(extentId uint64, offset, size int64, data []byte, crc uint32) (err error) {
	var e *Extent
	if e, err = s.getExtent(extentId); err != nil {
		return
	}
	if err = s.checkOffsetAndSize(offset, size); err != nil {
		return
	}
	if e.blocksCrc[MarkDeleteIndex] == MarkDelete {
		err = ErrorHasDelete
		return
	}

	e.readlock()
	defer e.readUnlock()
	if _, err = e.file.WriteAt(data[:size], offset+BlockCrcHeaderSize); err != nil {
		return
	}
	offsetInBlock := offset % BlockSize
	blockNo := offset / BlockSize
	if offsetInBlock != 0 {
		blockBuffer := make([]byte, BlockSize)
		e.file.ReadAt(blockBuffer, (blockNo)*BlockSize+BlockCrcHeaderSize)
		crc = crc32.ChecksumIEEE(blockBuffer)
	}
	binary.BigEndian.PutUint32(e.blocksCrc[blockNo*PerBlockCrcSize:(blockNo+1)*PerBlockCrcSize], crc)
	if _, err = e.file.WriteAt(e.blocksCrc[blockNo*PerBlockCrcSize:(blockNo+1)*PerBlockCrcSize], blockNo*PerBlockCrcSize); err != nil {
		return
	}
	if offsetInBlock+size <= BlockSize {
		return
	}

	nextBlockNo := blockNo + 1
	if offsetInBlock+size > BlockSize {
		nextBlockBuffer := make([]byte, BlockSize)
		e.file.ReadAt(nextBlockBuffer, (nextBlockNo)*BlockSize+BlockCrcHeaderSize)
		crc = crc32.ChecksumIEEE(nextBlockBuffer)
		binary.BigEndian.PutUint32(e.blocksCrc[(nextBlockNo)*PerBlockCrcSize:(nextBlockNo+1)*PerBlockCrcSize], crc)
		if _, err = e.file.WriteAt(e.blocksCrc[(nextBlockNo)*PerBlockCrcSize:(nextBlockNo+1)*PerBlockCrcSize], (nextBlockNo)*PerBlockCrcSize); err != nil {
			return
		}
	}

	return
}

func (s *ExtentStore) checkOffsetAndSize(offset, size int64) error {
	if offset+size > BlockSize*BlockCount {
		return ErrorUnmatchPara
	}
	if offset >= BlockCount*BlockSize || size == 0 {
		return ErrorUnmatchPara
	}

	if size > BlockSize {
		return ErrorUnmatchPara
	}

	return nil
}

func (s *ExtentStore) Read(extentId uint64, offset, size int64, nbuf []byte) (crc uint32, err error) {
	var e *Extent
	if e, err = s.getExtent(extentId); err != nil {
		return
	}
	if err = s.checkOffsetAndSize(offset, size); err != nil {
		return
	}
	if e.blocksCrc[MarkDeleteIndex] == MarkDelete {
		err = ErrorHasDelete
		return
	}
	offsetInBlock := offset % BlockSize
	e.readlock()
	defer e.readUnlock()
	if _, err = e.file.ReadAt(nbuf[:size], offset+BlockCrcHeaderSize); err != nil {
		return
	}
	blockNo := offset / BlockSize
	if offsetInBlock == 0 && size == BlockSize {
		crc = binary.BigEndian.Uint32(e.blocksCrc[blockNo*4 : (blockNo+1)*4])
	} else {
		crc = crc32.ChecksumIEEE(nbuf)
	}

	return
}

func (s *ExtentStore) MarkDelete(extentId uint64, offset, size int64) (err error) {
	var e *Extent
	if e, err = s.getExtent(extentId); err != nil {
		return
	}

	e.readlock()
	defer e.readUnlock()
	e.blocksCrc[MarkDeleteIndex] = MarkDelete
	if _, err = e.file.WriteAt(e.blocksCrc, 0); err != nil {
		return
	}

	return
}

func (s *ExtentStore) Delete(extentId uint64) (err error) {
	var e *Extent
	if e, err = s.getExtent(extentId); err != nil {
		return nil
	}

	s.delExtentFromCache(e)
	if err = e.deleteExtent(); err != nil {
		return nil
	}

	return
}

func (s *ExtentStore) IsMarkDelete(extentId uint64) (isMarkDelete bool, err error) {
	var e *Extent
	if e, err = s.getExtent(extentId); err != nil {
		return
	}
	isMarkDelete = e.blocksCrc[MarkDeleteIndex] == MarkDelete

	return
}

func (s *ExtentStore) Sync(extentId uint64) (err error) {
	var e *Extent
	if e, err = s.getExtent(extentId); err != nil {
		return
	}
	e.readlock()
	defer e.readUnlock()

	return e.file.Sync()
}

func (s *ExtentStore) SyncAll() { /*notici this function must called on program exit or kill */
	s.lock.Lock()
	defer s.lock.Unlock()
	for _, v := range s.extents {
		v.readlock()
		v.file.Sync()
		v.readUnlock()
	}
}

func (s *ExtentStore) GetBlockCrcBuffer(extentId uint64, headerBuff []byte) (err error) {
	var e *Extent
	if e, err = s.getExtent(extentId); err != nil {
		return
	}

	if len(headerBuff) != BlockCrcHeaderSize {
		return errors.New("header buff is not BlockCrcHeaderSize")
	}

	e.readlock()
	_, err = e.file.ReadAt(headerBuff, 0)
	e.readUnlock()

	return
}

func (s *ExtentStore) GetWatermark(extentId uint64) (extentInfo *FileInfo, err error) {
	var (
		e     *Extent
		finfo os.FileInfo
	)
	if e, err = s.getExtent(extentId); err != nil {
		return
	}
	e.readlock()
	defer e.readUnlock()

	finfo, err = e.file.Stat()
	if err != nil {
		return
	}
	size := finfo.Size() - BlockCrcHeaderSize
	extentInfo = &FileInfo{FileId: int(extentId), Size: uint64(size)}

	return
}

func (s *ExtentStore) GetAllWatermark() (extents []*FileInfo, err error) {
	extents = make([]*FileInfo, 0)
	var (
		finfos   []os.FileInfo
		extentId uint64
	)
	finfos, err = ioutil.ReadDir(s.dataDir)
	if err != nil {
		return
	}
	for _, finfo := range finfos {
		if time.Now().Unix()-finfo.ModTime().Unix() < 60*5 {
			continue
		}
		extentId, err = strconv.ParseUint(finfo.Name(), 10, 2)
		if err != nil {
			continue
		}
		var einfo *FileInfo
		einfo, err = s.GetWatermark(extentId)
		if err != nil {
			continue
		}
		extents = append(extents, einfo)
	}

	return
}

func (s *ExtentStore) extentExist(extentId uint64) (exist bool) {
	name := s.dataDir + "/" + strconv.Itoa((int)(extentId))
	if _, err := os.Stat(name); err == nil {
		exist = true
		warterMark, err := s.GetWatermark(extentId)
		if err == io.EOF || warterMark.Size < BlockCrcHeaderSize {
			err = s.fillBlockCrcHeader(name, BlockSize)
		}
	}

	return
}

func (s *ExtentStore) fillBlockCrcHeader(name string, blockSize int64) (err error) {
	if fp, err := os.OpenFile(name, os.O_RDWR|os.O_EXCL, 0666); err == nil {
		emptyCrc := crc32.ChecksumIEEE(make([]byte, blockSize))
		extentCrc := make([]byte, BlockCrcHeaderSize)
		for blockNo := 0; blockNo < BlockCount; blockNo++ {
			binary.BigEndian.PutUint32(extentCrc[blockNo*4:(blockNo+1)*4], emptyCrc)
		}
		_, err = fp.WriteAt(extentCrc, 0)
		fp.Close()
	}

	return
}

func (s *ExtentStore) GetStoreFileCount() (files int, err error) {
	var finfos []os.FileInfo

	if finfos, err = ioutil.ReadDir(s.dataDir); err == nil {
		files = len(finfos)
	}

	return
}

func (s *ExtentStore) GetStoreUsedSize() (size int64) {
	if finfoArray, err := ioutil.ReadDir(s.dataDir); err == nil {
		for _, finfo := range finfoArray {
			size += finfo.Size()
		}
	}

	return
}
