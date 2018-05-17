package datanode

import (
	"fmt"
	"github.com/juju/errors"
	"github.com/tiglabs/baudstorage/proto"
	"github.com/tiglabs/baudstorage/storage"
	"github.com/tiglabs/baudstorage/util/config"
	"github.com/tiglabs/baudstorage/util/log"
	"github.com/tiglabs/baudstorage/util/pool"
)

var (
	ErrStoreTypeUnmatch   = errors.New("store type error")
	ErrVolNotExist        = errors.New("vol not exsits")
	ErrChunkOffsetUnmatch = errors.New("chunk offset not unmatch")
)

type DataNode struct {
	ConnPool *pool.ConnPool
	space    *SpaceManager
}

func (n *DataNode) Start(cfg *config.Config) error {
	panic("implement me")
}

func (n *DataNode) Shutdown() {
	panic("implement me")
}

func (n *DataNode) Sync() {
	panic("implement me")
}

func NewServer() *DataNode {
	return &DataNode{}
}

func (s *DataNode) AddCompactTask(t *CompactTask) (err error) {
	v := s.space.getVol(t.volId)
	if v == nil {
		return nil
	}
	d, _ := s.space.getDisk(v.path)
	if d == nil {
		return nil
	}
	err = d.addTask(t)
	if err != nil {
		err = errors.Annotatef(err, "Task[%v] ", t.toString())
	}

	return
}

func (s *DataNode) checkChunkInfo(pkg *Packet) (err error) {
	chunkInfo, _ := pkg.vol.store.(*storage.TinyStore).GetWatermark(pkg.FileID)
	leaderObjId := uint64(pkg.Offset)
	localObjId := chunkInfo.Size
	if (leaderObjId - 1) != chunkInfo.Size {
		err = ErrChunkOffsetUnmatch
		mesg := fmt.Sprintf("Err[%v] leaderObjId[%v] localObjId[%v]", err, leaderObjId, localObjId)
		log.LogWarn(pkg.actionMesg(ActionCheckChunkInfo, LocalProcessAddr, pkg.StartT, fmt.Errorf(mesg)))
	}

	return
}

func (s *DataNode) handleChunkInfo(pkg *Packet) (err error) {
	if !pkg.IsWriteOperation() {
		return
	}

	if !pkg.isHeadNode() {
		err = s.checkChunkInfo(pkg)
	} else {
		err = s.headNodeSetChunkInfo(pkg)
	}
	if err != nil {
		pkg.PackErrorBody(ActionCheckChunkInfo, err.Error())
	}

	return
}

func (s *DataNode) headNodeSetChunkInfo(pkg *Packet) (err error) {
	var (
		chunkId int
	)
	store := pkg.vol.store.(*storage.TinyStore)
	chunkId, err = store.GetChunkForWrite()
	if err != nil {
		pkg.vol.status = storage.ReadOnlyStore
		return
	}
	pkg.FileID = uint64(chunkId)
	objectId, _ := store.AllocObjectId(uint32(pkg.FileID))
	pkg.Offset = int64(objectId)

	return
}

func (s *DataNode) headNodePutChunk(pkg *Packet) {
	if pkg == nil || pkg.FileID <= 0 || pkg.isReturn {
		return
	}
	if pkg.StoreType != proto.TinyStoreMode || !pkg.isHeadNode() || !pkg.IsWriteOperation() || !pkg.IsTransitPkg() {
		return
	}
	store := pkg.vol.store.(*storage.TinyStore)
	if pkg.IsErrPack() {
		store.PutUnAvailChunk(int(pkg.FileID))
	} else {
		store.PutAvailChunk(int(pkg.FileID))
	}
	pkg.isReturn = true
}
