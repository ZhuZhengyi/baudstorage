package metanode

import (
	"sync"
	"time"

	"github.com/google/btree"
	"github.com/tiglabs/baudstorage/proto"
	"github.com/tiglabs/baudstorage/raftopt"
	"github.com/tiglabs/baudstorage/sdk/stream"
)

type MetaRangeFsm struct {
	dLock      sync.RWMutex
	DentryTree *btree.BTree
	iLock      sync.RWMutex
	InodeTree  *btree.BTree
	raftopt.RaftStoreFsm
}

func NewMetaRangeFsm() *MetaRangeFsm {
	m := new(MetaRangeFsm)
	m.DentryTree = btree.New(32)
	m.InodeTree = btree.New(32)
	return m
}

// GetDentry query dentry from DentryTree with specified dentry info;
// if it exist, the required parameter is the dentry entity,
// if not exist, not change
func (mf *MetaRangeFsm) GetDentry(dentry *Dentry) (status uint8) {
	status = proto.OpOk
	item := mf.DentryTree.Get(dentry)
	if item == nil {
		status = proto.OpNotExistErr
		return
	}
	dentry = item.(*Dentry)
	return
}

// GetInode query inode from InodeTree with specified inode info;
// if it exist, the required parameter is the inode entity,
// if not exist, not change
func (mf *MetaRangeFsm) GetInode(ino *Inode) (status uint8) {
	status = proto.OpOk
	item := mf.InodeTree.Get(ino)
	if item == nil {
		status = proto.OpNotExistErr
		return
	}
	ino = item.(*Inode)
	return
}

//CreateDentry insert dentry into dentry tree
func (mf *MetaRangeFsm) CreateDentry(dentry *Dentry) (status uint8) {
	status = proto.OpOk
	mf.dLock.Lock()
	if mf.DentryTree.Has(dentry) {
		mf.dLock.Unlock()
		status = proto.OpExistErr
		return
	}
	mf.DentryTree.ReplaceOrInsert(dentry)
	mf.dLock.Unlock()
	//TODO: raft sync

	return
}

//DeleteDentry delete dentry from dentry tree
func (mf *MetaRangeFsm) DeleteDentry(dentry *Dentry) (status uint8) {
	status = proto.OpOk
	mf.dLock.Lock()
	item := mf.DentryTree.Delete(dentry)
	mf.dLock.Unlock()
	if item == nil {
		status = proto.OpExistErr
		return
	}
	dentry = item.(*Dentry)
	//TODO: raft sync

	return
}

func (mf *MetaRangeFsm) CreateInode(ino *Inode) (status uint8) {
	status = proto.OpOk
	mf.iLock.Lock()
	if mf.InodeTree.Has(ino) {
		mf.iLock.Unlock()
		status = proto.OpExistErr
		return
	}
	mf.InodeTree.ReplaceOrInsert(ino)
	mf.iLock.Unlock()
	//TODO: raft sync

	return
}

func (mf *MetaRangeFsm) DeleteInode(ino *Inode) (status uint8) {
	status = proto.OpOk
	mf.iLock.Lock()
	item := mf.InodeTree.Delete(ino)
	mf.iLock.Unlock()
	if item == nil {
		status = proto.OpNotExistErr
		return
	}
	ino = item.(*Inode)
	//TODO: raft sync
	return
}

func (mf *MetaRangeFsm) OpenFile(req *openReq) (resp *openResp) {
	item := mf.InodeTree.Get(&Inode{
		Inode: req.Inode,
	})
	if item == nil {
		resp.Status = int(proto.OpNotExistErr)
		return
	}
	item.(*Inode).AccessTime = time.Now().Unix()
	resp.Status = int(proto.OpOk)
	//TODO: raft sync

	return
}

func (mf *MetaRangeFsm) ReadDir(req *readdirReq) (resp *readdirResp) {
	begDentry := &Dentry{
		ParentId: req.ParentID,
	}
	endDentry := &Dentry{
		ParentId: req.ParentID + 1,
	}
	mf.DentryTree.AscendRange(begDentry, endDentry, func(i btree.Item) bool {
		d := i.(*Dentry)
		resp.Children = append(resp.Children, proto.Dentry{
			Inode: d.Inode,
			Type:  d.Type,
			Name:  d.Name,
		})
		return true
	})
	return
}

func (mf *MetaRangeFsm) PutStreamKey(ino *Inode, k stream.ExtentKey) (status uint8) {
	status = proto.OpOk
	item := mf.InodeTree.Get(ino)
	if item == nil {
		status = proto.OpNotExistErr
		return
	}
	ino = item.(*Inode)
	ino.Stream.Put(k)
	return
}
