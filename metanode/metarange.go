package metanode

import (
	"sync/atomic"
	"time"

	"github.com/tiglabs/baudstorage/proto"
	"github.com/tiglabs/baudstorage/sdk/stream"
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

func (mr *MetaRange) getInodeID() (uint64, uint8) {
	i := atomic.AddUint64(&mr.offset, 1)
	if i > mr.end {
		return -1, proto.OpInodeFullErr
	}
	return i, proto.OpOk
}

func (mr *MetaRange) CreateDentry(req *createDentryReq) (resp *createDentryResp) {
	// TODO: Implement create dentry operation.
	dentry := &Dentry{
		ParentId: req.ParentID,
		Name:     req.Name,
		Inode:    req.Inode,
		Type:     req.Mode,
	}
	status := mr.store.CreateDentry(dentry)
	resp.Status = int(status)
	return
}

func (mr *MetaRange) DeleteDentry(req *deleteDentryReq) (resp *deleteDentryResp) {
	//TODO: Implement delete dentry
	dentry := &Dentry{
		ParentId: req.ParentID,
		Name:     req.Name,
	}
	status := mr.store.DeleteDentry(dentry)
	resp.Status = int(status)
	return
}

func (mr *MetaRange) CreateInode(req *createInoReq) (resp *createInoResp) {
	//TODO: Implement create inode
	var status uint8
	resp.Inode, status = mr.getInode()
	if status != proto.OpOk {
		resp.Status = int(status)
		return
	}
	ts := time.Now().Unix()
	ino := &Inode{
		Inode:      resp.Inode,
		Name:       req.Name,
		Type:       req.Mode,
		AccessTime: ts,
		ModifyTime: ts,
		Stream:     stream.NewStreamKey(resp.Inode),
	}
	status = mr.store.CreateInode(ino)
	resp.Status = int(status)
	if status != proto.OpOk {
		resp.Inode = -1
	}
	return
}

func (mr *MetaRange) DeleteInode(req *deleteInoReq) (resp *deleteInoResp) {
	//TODO: Implement delete inode
	ino := &Inode{
		Inode: req.Inode,
	}
	resp.Status = int(mr.store.DeleteInode(ino))
	return
}

func (mr *MetaRange) UpdateInodeName(req *updateInoNameReq) (resp *updateInoNameResp) {
	ino := &Inode{
		Inode: req.Inode,
	}
	status := mr.store.GetInode(ino)
	if status == proto.OpOk {
		ino.Name = req.Name
	}
	resp.Status = int(status)
	return
}

func (mr *MetaRange) PutStreamKey() {
	return
}

func (mr *MetaRange) ReadDir(req *readdirReq) (resp *readdirResp) {
	// TODO: Implement read dir operation.
	resp = mr.store.ReadDir(req)
	return
}

func (mr *MetaRange) Open(req *openReq) (resp *openResp) {
	// TODO: Implement open operation.
	resp = mr.store.OpenFile(req)
	return
}
