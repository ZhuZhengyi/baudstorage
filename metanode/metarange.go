package metanode

import (
	"github.com/tiglabs/baudstorage/proto"
	"sync/atomic"
	"time"
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
	if v := mr.store.LookupDentry(dentry); v != nil {
		//TODO: file or dir existed

		return
	}
	t := time.Now().Unix()
	inode := &Inode{
		Inode:      mr.getInode(),
		Name:       request.Name,
		Type:       request.Mode,
		AccessTime: t,
		ModifyTime: t,
	}
	dentry.Inode = inode.Inode
	return mr.store.Create(inode, dentry)
}
