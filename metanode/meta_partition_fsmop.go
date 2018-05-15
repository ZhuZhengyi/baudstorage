package metanode

import (
	"time"

	"github.com/google/btree"
	"github.com/tiglabs/baudstorage/proto"
	"github.com/tiglabs/baudstorage/sdk/stream"
)

func NewMetaPartitionFsm(mr *MetaPartition) *MetaPartitionFsm {
	return &MetaPartitionFsm{
		metaPartition: mr,
		dentryTree:    btree.New(defaultBTreeDegree),
		inodeTree:     btree.New(defaultBTreeDegree),
	}
}

// GetDentry query dentry from DentryTree with specified dentry info;
// if it exist, the required parameter is the dentry entity,
// if not exist, not change
func (mf *MetaPartitionFsm) GetDentry(dentry *Dentry) (status uint8) {
	status = proto.OpOk
	item := mf.dentryTree.Get(dentry)
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
func (mf *MetaPartitionFsm) GetInode(ino *Inode) (status uint8) {
	status = proto.OpOk
	item := mf.inodeTree.Get(ino)
	if item == nil {
		status = proto.OpNotExistErr
		return
	}
	ino = item.(*Inode)
	return
}

func (mf *MetaPartitionFsm) GetInodeTree() *btree.BTree {
	return mf.inodeTree
}

// CreateDentry insert dentry into dentry tree.
func (mf *MetaPartitionFsm) CreateDentry(dentry *Dentry) (status uint8) {
	// TODO: Implement it.
	status = proto.OpOk
	mf.dentryMu.Lock()
	if mf.dentryTree.Has(dentry) {
		mf.dentryMu.Unlock()
		status = proto.OpExistErr
		return
	}
	mf.dentryTree.ReplaceOrInsert(dentry)
	mf.dentryMu.Unlock()
	return
}

// DeleteDentry delete dentry from dentry tree.
func (mf *MetaPartitionFsm) DeleteDentry(dentry *Dentry) (status uint8) {
	// TODO: Implement it.
	status = proto.OpOk
	mf.dentryMu.Lock()
	item := mf.dentryTree.Delete(dentry)
	mf.dentryMu.Unlock()
	if item == nil {
		status = proto.OpExistErr
		return
	}
	dentry = item.(*Dentry)
	return
}

func (mf *MetaPartitionFsm) GetDentryTree() *btree.BTree {
	return mf.dentryTree
}

// CreateInode create inode to inode tree.
func (mf *MetaPartitionFsm) CreateInode(ino *Inode) (status uint8) {
	// TODO: Implement it.
	status = proto.OpOk
	mf.inodeMu.Lock()
	if mf.inodeTree.Has(ino) {
		mf.inodeMu.Unlock()
		status = proto.OpExistErr
		return
	}
	mf.inodeTree.ReplaceOrInsert(ino)
	mf.inodeMu.Unlock()
	return
}

// DeleteInode delete specified inode item from inode tree.
func (mf *MetaPartitionFsm) DeleteInode(ino *Inode) (status uint8) {
	// TODO: Implement it.
	status = proto.OpOk
	mf.inodeMu.Lock()
	item := mf.inodeTree.Delete(ino)
	mf.inodeMu.Unlock()
	if item == nil {
		status = proto.OpNotExistErr
		return
	}
	ino = item.(*Inode)
	return
}

func (mf *MetaPartitionFsm) OpenFile(req *OpenReq) (status uint8) {
	item := mf.inodeTree.Get(&Inode{
		Inode: req.Inode,
	})
	if item == nil {
		status = proto.OpNotExistErr
		return
	}
	item.(*Inode).AccessTime = time.Now()
	status = proto.OpOk
	return
}

func (mf *MetaPartitionFsm) ReadDir(req *ReadDirReq) (resp *ReadDirResp) {
	begDentry := &Dentry{
		ParentId: req.ParentID,
	}
	endDentry := &Dentry{
		ParentId: req.ParentID + 1,
	}
	mf.dentryTree.AscendRange(begDentry, endDentry, func(i btree.Item) bool {
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

func (mf *MetaPartitionFsm) PutStreamKey(ino *Inode, k stream.ExtentKey) (status uint8) {
	status = proto.OpOk
	item := mf.inodeTree.Get(ino)
	if item == nil {
		status = proto.OpNotExistErr
		return
	}
	ino = item.(*Inode)
	ino.Stream.Put(k)
	return
}
