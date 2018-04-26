package metanode

import (
	"github.com/google/btree"
	"github.com/tiglabs/baudstorage/proto"
	"github.com/tiglabs/baudstorage/raftopt"
	"sync"
)

type MetaRangeFsm struct {
	dentryLock sync.Locker
	DentryTree *btree.BTree
	inodeLock  sync.Locker
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
func (mf *MetaRangeFsm) LookupDentry(dentry *Dentry) *Dentry {
	item := mf.DentryTree.Get(dentry)
	if item == nil {
		return nil
	}
	d := item.(*Dentry)
	return d
}

func (mr *MetaRangeFsm) Create(inode *Inode, dentry *Dentry) (response *proto.CreateResponse) {
	//TODO: first raft sync

	//insert inode tree
	mr.inodeLock.Lock()
	if mr.InodeTree.Has(inode) {
		defer mr.inodeLock.Unlock()
		//TODO: inode is already there.

		return
	}
	mr.InodeTree.ReplaceOrInsert(inode)
	mr.inodeLock.Unlock()

	//insert dentry tree
	mr.dentryLock.Lock()
	mr.DentryTree.ReplaceOrInsert(dentry)
	mr.dentryLock.Unlock()
	response.Status = int(proto.OpOk)
	response.Inode = dentry.Inode
	response.Name = dentry.Name
	response.Type = dentry.Type
	return
}

func (mr *MetaRangeFsm) Delete(request *proto.DeleteRequest) (response *proto.DeleteResponse) {
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
