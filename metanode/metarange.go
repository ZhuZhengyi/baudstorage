package metanode

import (
	"github.com/tiglabs/baudstorage/proto"
	"sync/atomic"
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

func (mr *MetaRange) getInode() uint64 {
	return atomic.AddUint64(&mr.offset, 1)
}

func (mr *MetaRange) Create(req *proto.CreateRequest) (resp *proto.CreateResponse) {
	dentry := &Dentry{
		ParentId: req.ParentId,
		Name:     req.Name,
		Type:     req.Mode,
	}
	inode := NewInode(mr.getInode(), req.Name, req.Mode)
	dentry.Inode = inode.Inode
	resp = &proto.CreateResponse{}
	resp.Status = int(mr.store.Create(inode, dentry))
	resp.Inode = inode.Inode
	resp.Name = req.Name
	resp.Type = req.Mode
	return
}

func (mr *MetaRange) Rename(req *proto.RenameRequest) (resp *proto.RenameResponse) {
	// TODO: Implement rename operation.
	return
}

func (mr *MetaRange) Delete(req *proto.DeleteRequest) (resp *proto.DeleteResponse) {
	// TODO: Implement delete operation.
	return
}

func (mr *MetaRange) ReadDir(req *proto.ReadDirRequest) (resp *proto.ReadDirResponse) {
	// TODO: Implement read dir operation.
	return
}

func (mr *MetaRange) Open(req *proto.OpenRequest) (resp *proto.OpenResponse) {
	// TODO: Implement open operation.
	return
}
