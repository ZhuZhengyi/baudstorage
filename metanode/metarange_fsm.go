package metanode

import (
	"github.com/google/btree"
	"github.com/tiglabs/baudstorage/proto"
	"github.com/tiglabs/baudstorage/raftopt"
	"sync"
)

type MetaRangeFsm struct {
	dLock      sync.Locker
	DentryTree *btree.BTree
	iLock      sync.Locker
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
		return nil, proto.OpFileExistErr
	}
	d := item.(*Dentry)
	return d, proto.OpOk
}

func (mr *MetaRangeFsm) GetInode(ino *Inode) (*Inode, uint8) {
	item := mr.InodeTree.Get(ino)
	if item == nil {
		return nil, proto.OpFileExistErr
	}
	i := item.(*Inode)
	return i, proto.OpOk
}

func (mr *MetaRangeFsm) GetStream(ino uint64) {

}

func (mr *MetaRangeFsm) Create(inode *Inode, dentry *Dentry) uint8 {
	//TODO: first raft sync

	//insert dentry tree
	mr.dLock.Lock()
	if mr.DentryTree.Has(dentry) {
		defer mr.dLock.Unlock()
		return proto.OpFileExistErr
	}
	mr.DentryTree.ReplaceOrInsert(dentry)
	mr.dLock.Unlock()
	//insert inode tree
	mr.iLock.Lock()
	if mr.InodeTree.Has(inode) {
		defer mr.iLock.Unlock()
		return proto.OpFileExistErr
	}
	mr.InodeTree.ReplaceOrInsert(inode)
	mr.iLock.Unlock()
	return proto.OpOk
}

func (mr *MetaRangeFsm) Delete(inode *Inode, dentry *Dentry) uint8 {
	//TODO: raft sync

	//delete dentry from dentrytree
	mr.dLock.Lock()
	if !mr.DentryTree.Has(inode) {
		defer mr.dLock.Unlock()
		return proto.OpFileExistErr
	}
	mr.DentryTree.Delete(dentry)
	mr.dLock.Unlock()
	//delete inode from inodetree
	mr.iLock.Lock()
	if !mr.InodeTree.Has(inode) {
		defer mr.iLock.Unlock()
		return proto.OpFileExistErr
	}
	mr.InodeTree.Delete(inode)
	mr.iLock.Unlock()
	return proto.OpOk
}

func (mr *MetaRangeFsm) OpenFile(request *proto.OpenFileRequest) (response *proto.OpenFileResponse) {
	return
}

func (mr *MetaRangeFsm) Rename(req *proto.RenameRequest) (response *proto.RenameResponse) {

	return
}

func (mr *MetaRangeFsm) List(request *proto.ReadDirRequest) (response *proto.ReadDirResponse) {

	return
}
