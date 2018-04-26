package storage

import (
	"fmt"
	"github.com/juju/errors"
	util "github.com/tiglabs/baudstorage/util"
	"os"
	"strconv"
	"syscall"
	"time"
)

const (
	ChunkCount        = 40
	ChunkOpenOpt      = os.O_CREATE | os.O_RDWR | os.O_APPEND
	ReadOnlyStore     = 1
	ReadWriteStore    = 2
	CompactThreshold  = 40
	CompactMaxWait    = time.Second * 10
	ReBootStoreMode   = false
	NewStoreMode      = true
	MinWriteAbleChunk = 1
)

var (
	ErrorObjNotFound    = errors.New("object not exist")
	ErrorChunkNotFound  = errors.New("chunk not exist")
	ErrorVolReadOnly    = errors.New("volume readonly")
	ErrorHasDelete      = errors.New("has delete")
	ErrorUnmatchPara    = errors.New("unmatch offset")
	ErrorNoAvaliFile    = errors.New("no avail file")
	ErrorNoUnAvaliFile  = errors.New("no Unavail file")
	ErrorNewStoreMode   = errors.New("error new store mode ")
	ErrExtentNameFormat = errors.New("extent filePath format error")
	ErrSyscallNoSpace   = errors.New("no space left on device")
	ErrorAgain          = errors.New("try again")
	ErrorCompaction     = errors.New("compaction error")
	ErrorCommit         = errors.New("commit error")
	ErrObjectSmaller    = errors.New("object smaller error")
)

/*
tiny Store contains 40 chunkFiles,when write ,choose a avaliChunkFile append
*/

type TinyStore struct {
	dataDir        string
	chunks         map[int]*Chunk
	availChunkCh   chan int
	unavailChunkCh chan int
	storeSize      int
	chunkSize      int
	fullChunks     *util.Set
}

func NewTinyStore(dataDir string, storeSize int, newMode bool) (s *TinyStore, err error) {
	s = new(TinyStore)
	s.dataDir = dataDir
	if err = CheckAndCreateSubdir(dataDir, newMode); err != nil {
		return nil, fmt.Errorf("NewTinyStore [%v] err[%v]", dataDir, err)
	}
	s.chunks = make(map[int]*Chunk)
	if err = s.initChunkFile(); err != nil {
		return nil, fmt.Errorf("NewTinyStore [%v] err[%v]", dataDir, err)
	}

	s.availChunkCh = make(chan int, ChunkCount+1)
	s.unavailChunkCh = make(chan int, ChunkCount+1)
	for i := 1; i <= ChunkCount; i++ {
		s.unavailChunkCh <- i
	}
	s.storeSize = storeSize
	s.chunkSize = storeSize / ChunkCount
	s.fullChunks = util.NewSet()

	return
}

func (s *TinyStore) GetStoreStatus() int {
	if ChunkCount-s.fullChunks.Len() <= MinWriteAbleChunk {
		return ReadOnlyStore
	}

	return ReadWriteStore
}

func (s *TinyStore) DeleteStore() {
	for index, c := range s.chunks {
		c.file.Close()
		c.tree.idxFile.Close()
		delete(s.chunks, index)
	}
	os.RemoveAll(s.dataDir)
}

func (s *TinyStore) initChunkFile() (err error) {
	for i := 1; i <= ChunkCount; i++ {
		var c *Chunk
		if c, err = NewChunk(s.dataDir, i); err != nil {
			return fmt.Errorf("initChunkFile Error %s", err.Error())
		}
		s.chunks[i] = c
	}

	return
}

func (s *TinyStore) chunkExist(chunkId uint32) (exist bool) {
	name := s.dataDir + "/" + strconv.Itoa(int(chunkId))
	if _, err := os.Stat(name); err == nil {
		exist = true
	}

	return
}

func (s *TinyStore) WriteDeleteDentry(objectId uint64, chunkId int) (err error) {
	var (
		fi os.FileInfo
	)
	c, ok := s.chunks[chunkId]
	if !ok {
		return ErrorChunkNotFound
	}
	if !c.compactLock.TryLock() {
		return ErrorAgain
	}
	defer c.compactLock.Unlock()
	if fi, err = c.file.Stat(); err != nil {
		return
	}
	o := &Object{Oid: objectId, Size: TombstoneFileSize, Offset: uint32(fi.Size())}
	if err = c.tree.appendToIdxFile(o); err == nil {
		if c.loadLastOid() < objectId {
			c.storeLastOid(objectId)
		}
	}

	return
}

func (s *TinyStore) Write(fileId uint32, offset, size int64, data []byte, crc uint32) (err error) {
	var (
		fi os.FileInfo
	)
	chunkId := int(fileId)
	objectId := uint64(offset)
	c, ok := s.chunks[chunkId]
	if !ok {
		return ErrorChunkNotFound
	}

	if !c.compactLock.TryLock() {
		return ErrorAgain
	}
	defer c.compactLock.Unlock()

	if objectId < c.loadLastOid() {
		msg := fmt.Sprintf("Object id smaller than last oid. DataDir[%v] ChunkId[%v]"+
			" ObjectId[%v] LastOid[%v]", s.dataDir, chunkId, objectId, c.loadLastOid())
		err = errors.New(msg)
		return ErrObjectSmaller
	}

	if fi, err = c.file.Stat(); err != nil {
		return
	}

	newOffset := fi.Size()
	if _, err = c.file.WriteAt(data[:size], newOffset); err != nil {
		return
	}

	if _, _, err = c.tree.set(objectId, uint32(newOffset), uint32(size), crc); err == nil {
		if c.loadLastOid() < objectId {
			c.storeLastOid(objectId)
		}
	}
	return
}

func (s *TinyStore) Read(fileId uint32, offset, size int64, nbuf []byte) (crc uint32, err error) {
	chunkId := int(fileId)
	objectId := uint64(offset)
	c, ok := s.chunks[chunkId]
	if !ok {
		return 0, ErrorChunkNotFound
	}

	lastOid := c.loadLastOid()
	if lastOid < objectId {
		return 0, ErrorChunkNotFound
	}

	c.commitLock.RLock()
	defer c.commitLock.RUnlock()

	var fi os.FileInfo
	if fi, err = c.file.Stat(); err != nil {
		return
	}

	o, ok := c.tree.get(objectId)
	if !ok {
		return 0, ErrorObjNotFound
	}

	if int64(o.Size) != size || int64(o.Offset)+size > fi.Size() {
		return 0, ErrorUnmatchPara
	}

	if _, err = c.file.ReadAt(nbuf[:size], int64(o.Offset)); err != nil {
		return
	}
	crc = o.Crc

	return
}

func (s *TinyStore) Sync(fileId uint32) (err error) {
	chunkId := (int)(fileId)
	c, ok := s.chunks[chunkId]
	if !ok {
		return ErrorChunkNotFound
	}

	err = c.tree.idxFile.Sync()
	if err != nil {
		return
	}

	return c.file.Sync()
}

func (s *TinyStore) GetAllWatermark() (chunks []*ChunkInfo, err error) {
	chunks = make([]*ChunkInfo, 0)
	for chunkId, c := range s.chunks {
		ci := &ChunkInfo{ChunkId: chunkId, LastOid: c.loadLastOid()}
		chunks = append(chunks, ci)
	}

	return
}

func (s *TinyStore) GetWatermark(fileId uint32) (offset int64, err error) {
	chunkId := (int)(fileId)
	c, ok := s.chunks[chunkId]
	if !ok {
		return -1, ErrorChunkNotFound
	}
	offset = int64(c.loadLastOid())

	return
}

func (s *TinyStore) GetStoreUsedSize() (size int64) {
	var err error
	for _, c := range s.chunks {
		var finfo os.FileInfo
		if finfo, err = c.file.Stat(); err == nil {
			size += finfo.Size()
		}
	}

	return
}

func (s *TinyStore) GetAvailChunk() (chunkId int, err error) {
	select {
	case chunkId = <-s.availChunkCh:
	default:
		err = ErrorNoAvaliFile
	}

	return
}

func (s *TinyStore) SyncAll() {
	for _, chunkFp := range s.chunks {
		chunkFp.tree.idxFile.Sync()
		chunkFp.file.Sync()
	}
}

func (s *TinyStore) PutAvailChunk(chunkId int) {
	s.availChunkCh <- chunkId
}

func (s *TinyStore) GetUnAvailChunk() (chunkId int, err error) {
	select {
	case chunkId = <-s.unavailChunkCh:
	default:
		err = ErrorNoUnAvaliFile
	}

	return
}

func (s *TinyStore) PutUnAvailChunk(chunkId int) {
	s.unavailChunkCh <- chunkId
}

func (s *TinyStore) GetStoreChunkCount() (files int, err error) {
	return ChunkCount, nil
}

func (s *TinyStore) MarkDelete(fileId uint32, offset, size int64) error {
	chunkId := int(fileId)
	objectId := uint64(offset)
	c, ok := s.chunks[chunkId]
	if !ok {
		return ErrorChunkNotFound
	}

	return c.tree.delete(objectId)
}

func (s *TinyStore) GetUnAvailChanLen() (chanLen int) {
	return len(s.unavailChunkCh)
}

func (s *TinyStore) GetAvailChanLen() (chanLen int) {
	return len(s.availChunkCh)
}

func (s *TinyStore) AllocObjectId(fileId uint32) (uint64, error) {
	chunkId := int(fileId)
	c, ok := s.chunks[chunkId]
	if !ok {
		return 0, ErrorChunkNotFound //0 is an invalid object id
	}
	return c.loadLastOid() + 1, nil
}

func (s *TinyStore) GetLastOid(fileId uint32) (objectId uint64, err error) {
	c, ok := s.chunks[int(fileId)]
	if !ok {
		return 0, ErrorChunkNotFound
	}

	return c.loadLastOid(), nil
}

func (s *TinyStore) GetObject(fileId uint32, objectId uint64) (o *Object, err error) {
	c, ok := s.chunks[int(fileId)]
	if !ok {
		return nil, ErrorChunkNotFound
	}

	o, ok = c.tree.get(objectId)
	if !ok {
		return nil, ErrorObjNotFound
	}

	return
}

func (s *TinyStore) GetDelObjects(fileId uint32) (objects []uint64) {
	objects = make([]uint64, 0)
	c, ok := s.chunks[int(fileId)]
	if !ok {
		return
	}

	syncLastOid := c.loadLastOid()
	c.storeSyncLastOid(syncLastOid)

	c.commitLock.RLock()
	WalkIndexFile(c.tree.idxFile, func(oid uint64, offset, size, crc uint32) error {
		if oid > syncLastOid {
			return errors.New("Exceed syncLastOid")
		}
		if size == TombstoneFileSize {
			objects = append(objects, oid)
		}
		return nil
	})
	c.commitLock.RUnlock()

	return
}

func (s *TinyStore) ApplyDelObjects(fileId uint32, objects []uint64) (err error) {
	c, ok := s.chunks[int(fileId)]
	if !ok {
		return ErrorChunkNotFound
	}
	err = c.applyDelObjects(objects)
	return
}

func (s *TinyStore) UpdateStoreInfo() {
	for chunkId, c := range s.chunks {
		finfo, err := c.file.Stat()
		if err != nil {
			continue
		}
		if finfo.Size() >= int64(s.chunkSize) {
			s.fullChunks.Add(chunkId)
		} else {
			s.fullChunks.Remove(chunkId)
		}
	}

	return
}

func (s *TinyStore) DoCompactWork(chunkId int) (err error) {
	_, ok := s.chunks[chunkId]
	if !ok {
		return ErrorChunkNotFound
	}

	ok = s.IsReadyToCompact(chunkId)
	if !ok {
		return nil
	}

	err = s.doCompactAndCommit(chunkId)
	if err != nil {
		return err
	}

	return s.Sync(uint32(chunkId))
}

// make sure chunkId is valid
func (s *TinyStore) IsReadyToCompact(chunkId int) bool {
	c := s.chunks[chunkId]
	tree := c.tree

	if s.fullChunks.Has(chunkId) {
		if tree.fileBytes < uint64(s.chunkSize) {
			return true
		} else {
			return false
		}
	}

	if tree.deleteBytes*100/(tree.fileBytes+1) >= uint64(CompactThreshold) {
		return true
	}

	return false
}

// make sure chunkId is valid
func (s *TinyStore) doCompactAndCommit(chunkId int) (err error) {
	c := s.chunks[chunkId]
	if !c.compactLock.TryLockTimed(CompactMaxWait) {
		return nil
	}
	defer c.compactLock.Unlock()

	if err = c.doCompact(); err != nil {
		return ErrorCompaction
	}

	c.commitLock.Lock()
	defer c.commitLock.Unlock()

	err = c.doCommit()
	if err != nil {
		return ErrorCommit
	}

	return nil
}

func CheckAndCreateSubdir(name string, newMode bool) (err error) {
	var fd int
	if newMode == ReBootStoreMode {
		err = os.MkdirAll(name, 0755)
	} else if newMode == NewStoreMode {
		err = os.Mkdir(name, 0755)
	} else {
		err = ErrorNewStoreMode
	}
	if err != nil {
		return
	}

	if fd, err = syscall.Open(name, os.O_RDONLY, 0666); err != nil {
		return
	}

	if err = syscall.Fsync(fd); err != nil {
		return
	}
	syscall.Close(fd)

	return
}
