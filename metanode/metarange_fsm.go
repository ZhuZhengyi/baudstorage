package metanode

import (
	"sync"

	"time"

	"github.com/google/btree"
	"github.com/tiglabs/baudstorage/proto"
	"github.com/tiglabs/baudstorage/raftopt"
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

// looks for the dentry in the DentryTree, returning it.
// returns nil if unable to find that dentry.
func (mr *MetaRangeFsm) GetDentry(dentry *Dentry) (*Dentry, uint8) {
	item := mr.DentryTree.Get(dentry)
	if item == nil {
		return nil, proto.OpFileNotExistErr
	}
	d := item.(*Dentry)
	return d, proto.OpOk
}

func (mr *MetaRangeFsm) GetInode(ino *Inode) (*Inode, uint8) {
	item := mr.InodeTree.Get(ino)
	if item == nil {
		return nil, proto.OpFileNotExistErr
	}
	i := item.(*Inode)
	return i, proto.OpOk
}

func (mr *MetaRangeFsm) AddStream(ino uint64) {

}

func (mr *MetaRangeFsm) Create(inode *Inode, dentry *Dentry) uint8 {
	//TODO: first raft sync

	//insert dentry tree
	mr.dLock.Lock()
	if mr.DentryTree.Has(dentry) {
		mr.dLock.Unlock()
		return proto.OpFileExistErr
	}
	mr.DentryTree.ReplaceOrInsert(dentry)
	mr.dLock.Unlock()
	//insert inode tree
	mr.iLock.Lock()
	if mr.InodeTree.Has(inode) {
		mr.iLock.Unlock()
		return proto.OpFileExistErr
	}
	mr.InodeTree.ReplaceOrInsert(inode)
	mr.iLock.Unlock()
	return proto.OpOk
}

func (mr *MetaRangeFsm) Delete(dentry *Dentry) uint8 {
	//TODO: raft sync

	//delete dentry from dentrytree
	mr.dLock.Lock()
	if item := mr.DentryTree.Delete(dentry); item == nil {
		defer mr.dLock.Unlock()
		return proto.OpFileNotExistErr
	} else {
		dentry = item.(*Dentry)
	}
	mr.dLock.Unlock()
	//delete inode from inodetree
	mr.iLock.Lock()
	if item := mr.InodeTree.Delete(&Inode{Inode: dentry.Inode}); item == nil {
		defer mr.iLock.Unlock()
		return proto.OpFileNotExistErr
	}
	mr.iLock.Unlock()
	return proto.OpOk
}

func (mr *MetaRangeFsm) OpenFile(request *proto.OpenFileRequest) (response *proto.OpenFileResponse) {
	item := mr.InodeTree.Get(&Inode{
		Inode: request.Inode,
	})
	if item == nil {
		response.Status = int(proto.OpFileNotExistErr)
		return
	}
	item.(*Inode).AccessTime = time.Now().Unix()
	response.Status = int(proto.OpOk)
	return
}

func (mr *MetaRangeFsm) Rename(req *proto.RenameRequest) (response *proto.RenameResponse) {
	dentry := &Dentry{
		ParentId: req.SrcParentId,
		Name:     req.SrcName,
	}
	dentry, status := mr.GetDentry(dentry)
	if status != proto.OpOk {
		response.Status = int(status)
		return
	}
	//rename inode name
	{
		inode := &Inode{
			Inode: dentry.Inode,
		}
		inode, status := mr.GetInode(inode)
		if status != proto.OpOk {
			response.Status = int(status)
			return
		}
		inode.Name = req.DstName
	}
	//delete old dentry from dentry tree
	mr.dLock.Lock()
	mr.DentryTree.Delete(dentry)
	mr.dLock.Unlock()
	//insert new dentry to dentry tree
	dentry.ParentId = req.DstParentId
	dentry.Name = req.DstName
	mr.dLock.Lock()
	mr.DentryTree.ReplaceOrInsert(dentry)
	//TODO: raft sync

	//response status
	response.Status = int(proto.OpOk)
	return
}

func (mr *MetaRangeFsm) List(request *proto.ReadDirRequest) (response *proto.ReadDirResponse) {
	begDentry := &Dentry{
		ParentId: request.ParentId,
	}
	endDentry := &Dentry{
		ParentId: request.ParentId + 1,
	}
	mr.DentryTree.AscendRange(begDentry, endDentry, func(i btree.Item) bool {
		d := i.(*Dentry)
		response.Children = append(response.Children, proto.Dentry{
			Inode: d.Inode,
			Type:  d.Type,
			Name:  d.Name,
		})
		return true
	})
	return
}
