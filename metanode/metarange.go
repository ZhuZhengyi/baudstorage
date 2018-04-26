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

func (mr *MetaRange) Create(request *proto.CreateRequest) (response *proto.CreateResponse) {
	dentry := &Dentry{
		ParentId: request.ParentId,
		Name:     request.Name,
		Type:     request.Mode,
	}
	if v := mr.store.GetDentry(dentry); v != nil {
		//TODO: file or dir existed

		return
	}

	inode := NewInode(mr.getInode(), request.Name, request.Mode)
	dentry.Inode = inode.Inode
	return mr.store.Create(inode, dentry)
}
