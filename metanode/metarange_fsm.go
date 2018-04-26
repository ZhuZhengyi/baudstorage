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
func (mr *MetaRangeFsm) GetDentry(dentry *Dentry) *Dentry {
	item := mr.DentryTree.Get(dentry)
	if item == nil {
		return nil
	}
	d := item.(*Dentry)
	return d
}

func (mr *MetaRangeFsm) GetInode(ino *Inode) *Inode {
	item := mr.InodeTree.Get(ino)
	if item == nil {
		return nil
	}
	i := item.(*Inode)
	return i
}

func (mr *MetaRangeFsm) GetStream(ino uint64) {

}

func (mr *MetaRangeFsm) Create(inode *Inode, dentry *Dentry) (response *proto.CreateResponse) {
	//TODO: first raft sync

	//insert inode tree
	mr.iLock.Lock()
	if mr.InodeTree.Has(inode) {
		defer mr.iLock.Unlock()
		//TODO: inode is already there.

		return
	}
	mr.InodeTree.ReplaceOrInsert(inode)
	mr.iLock.Unlock()

	//insert dentry tree
	mr.dLock.Lock()
	mr.DentryTree.ReplaceOrInsert(dentry)
	mr.dLock.Unlock()

	//TODO:
	return
}

func (mr *MetaRangeFsm) Delete(inode *Inode, dentry *Dentry) (response *proto.DeleteResponse) {
	if !mr.DentryTree.Has(dentry) {
		return
	}
	mr.dLock.Lock()
	mr.DentryTree.Delete(dentry)
	mr.dLock.Unlock()
	mr.iLock.Lock()
	mr.InodeTree.Delete(inode)
	mr.iLock.Unlock()
	return
}

func (mr *MetaRangeFsm) OpenFile(request *proto.OpenFileRequest) (response *proto.OpenFileResponse) {
	return
}

func (mr *MetaRangeFsm) Rename(request *proto.RenameRequest) (response *proto.RenameResponse) {

	return
}

func (mr *MetaRangeFsm) List(request *proto.ReadDirRequest) (response *proto.ReadDirResponse) {
	return
}
