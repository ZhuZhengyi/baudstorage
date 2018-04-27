package metanode

import (
	"sync/atomic"

	"errors"
	"github.com/tiglabs/baudstorage/proto"
)

type MetaRange struct {
	id          string // Consist with 'namespace_start_end'
	start       uint64
	end         uint64
	store       *MetaRangeFsm
	offset      uint64
	peers       []string
	raftGroupId uint64
	status      int
}

func NewMetaRange(id string, start, end uint64, peers []string) *MetaRange {
	return &MetaRange{
		id:     id,
		start:  start,
		end:    end,
		store:  NewMetaRangeFsm(),
		offset: start,
		peers:  peers,
	}
}

func (mr *MetaRange) getInode() (uint64, error) {
	i := atomic.AddUint64(&mr.offset, 1)
	if i > mr.end {
		return -1, errors.New("inode no space left")
	}
	return i, nil
}

func (mr *MetaRange) CreateDentry(request *proto.CreateRequest) (
	response *proto.CreateResponse) {
	// TODO: Implement create dentry operation.
	return
}

func (mr *MetaRange) DeleteDentry() {
	//TODO:
	return
}

func (mr *MetaRange) CreateInode() {
	//TODO:
	return
}

func (mr *MetaRange) DeleteInode() {
	//TODO:
	return
}

func (mr *MetaRange) UpdateInodeName() {
	return
}

func (mr *MetaRange) PutStreamKey() {
	return
}

func (mr *MetaRange) ReadDir(req *proto.ReadDirRequest) (resp *proto.ReadDirResponse) {
	// TODO: Implement read dir operation.
	resp = mr.store.ReadDir(req)
	return
}

func (mr *MetaRange) Open(req *proto.OpenRequest) (resp *proto.OpenResponse) {
	// TODO: Implement open operation.
	resp = mr.store.OpenFile(req)
	return
}
