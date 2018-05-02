package metanode

import (
	"github.com/juju/errors"
	"sync/atomic"
	"time"

	"github.com/tiglabs/baudstorage/proto"
	"github.com/tiglabs/baudstorage/sdk/stream"
)

// Errors
var (
	ErrInodeOutOfRange = errors.New("inode ID out of range.")
)

type MetaRange struct {
	id          string // Consist with 'namespace_start_end'.
	start       uint64
	end         uint64
	store       *MetaRangeFsm
	cur         uint64 // Cur ID value of inode what have been already assigned.
	peers       []string
	raftGroupId uint64
	status      int
}

func NewMetaRange(id string, start, end uint64, peers []string) *MetaRange {
	return &MetaRange{
		id:    id,
		start: start,
		end:   end,
		store: NewMetaRangeFsm(),
		cur:   start,
		peers: peers,
	}
}

// NextInodeId returns a new ID value of inode and update offset.
// If inode ID is out of this MetaRange limit then return ErrInodeOutOfRange error.
func (mr *MetaRange) nextInodeID() (newOffset uint64, err error) {
	for {
		cur := mr.cur
		end := mr.end
		if cur >= end {
			return 0, ErrInodeOutOfRange
		}
		newId := cur + 1
		if atomic.CompareAndSwapUint64(&mr.cur, cur, newId) {
			return newId, nil
		}
	}
}

func (mr *MetaRange) CreateDentry(req *CreateDentryReq) (resp *CreateDentryResp) {
	dentry := &Dentry{
		ParentId: req.ParentID,
		Name:     req.Name,
		Inode:    req.Inode,
		Type:     req.Mode,
	}
	status := mr.store.CreateDentry(dentry)
	resp.Status = status
	return
}

func (mr *MetaRange) DeleteDentry(req *DeleteDentryReq) (resp *DeleteDentryResp) {
	dentry := &Dentry{
		ParentId: req.ParentID,
		Name:     req.Name,
	}
	status := mr.store.DeleteDentry(dentry)
	resp.Status = status
	resp.Inode = dentry.Inode
	return
}

func (mr *MetaRange) CreateInode(req *CreateInoReq) (resp *CreateInoResp) {
	var err error
	resp.Inode, err = mr.nextInodeID()
	if err != nil {
		resp.Status = proto.OpInodeFullErr
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
	resp.Status = mr.store.CreateInode(ino)
	return
}

func (mr *MetaRange) DeleteInode(req *deleteInoReq) (resp *deleteInoResp) {
	ino := &Inode{
		Inode: req.Inode,
	}
	resp.Status = mr.store.DeleteInode(ino)
	return
}

func (mr *MetaRange) UpdateInodeName(req *UpdateInoNameReq) (
	resp *UpdateInoNameResp) {
	ino := &Inode{
		Inode: req.Inode,
	}
	status := mr.store.GetInode(ino)
	if status == proto.OpOk {
		ino.Name = req.Name
	}
	resp.Status = status
	return
}

func (mr *MetaRange) PutStreamKey() {
	return
}

func (mr *MetaRange) ReadDir(req *ReadDirReq) (resp *ReadDirResp) {
	// TODO: Implement read dir operation.
	resp = mr.store.ReadDir(req)
	return
}

func (mr *MetaRange) Open(req *OpenReq) (resp *OpenResp) {
	// TODO: Implement open operation.
	resp = mr.store.OpenFile(req)
	return
}
