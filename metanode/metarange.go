package metanode

import (
	"sync/atomic"

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

func (mr *MetaRange) getInode() uint64 {
	return atomic.AddUint64(&mr.offset, 1)
}

func (mr *MetaRange) Create(request *proto.CreateRequest) (response *proto.CreateResponse) {
	// TODO: Implement create operation.
	response.Status = int(proto.OpFileExistErr)
	dentry := &Dentry{
		ParentId: request.ParentId,
		Name:     request.Name,
		Type:     request.Mode,
	}
	if _, status := mr.store.GetDentry(dentry); status == proto.OpOk {
		return
	}

	inode := NewInode(mr.getInode(), request.Name, request.Mode)
	dentry.Inode = inode.Inode
	status := mr.store.Create(inode, dentry)
	if status == proto.OpOk {
		response.Status = int(proto.OpOk)
		response.Inode = dentry.Inode
		response.Name = dentry.Name
		response.Type = dentry.Type
	}
	return
}

func (mr *MetaRange) Rename(request *proto.RenameRequest) (response *proto.RenameResponse) {
	// TODO: Implement rename operation.
	return mr.store.Rename(request)
}

func (mr *MetaRange) Delete(request *proto.DeleteRequest) (response *proto.DeleteResponse) {
	dentry := &Dentry{
		ParentId: request.ParentId,
		Name:     request.Name,
	}
	response.Status = int(mr.store.Delete(dentry))
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
