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

// GetDentry query dentry item from DentryTree with specified dentry
// info and returns dentry entity if it exist, or nil.
func (mr *MetaRangeFsm) GetDentry(dentry *Dentry) (result *Dentry) {
	item := mr.DentryTree.Get(dentry)
	result = item.(*Dentry)
	return
}

// GetInode query inode entity from InodeTree with specified inode
// info and returns inode entity if it exist, or nil.
func (mr *MetaRangeFsm) GetInode(ino *Inode) (result *Inode) {
	item := mr.InodeTree.Get(ino)
	result = item.(*Inode)
	return
}

func (mr *MetaRangeFsm) GetStream(ino uint64) {
	// TODO: implement it.
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
