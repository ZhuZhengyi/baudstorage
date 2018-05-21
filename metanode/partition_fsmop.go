package metanode

import (
	"time"

	"github.com/google/btree"
	"github.com/tiglabs/baudstorage/proto"
)

// GetDentry query dentry from DentryTree with specified dentry info;
// if it exist, the required parameter is the dentry entity,
// if not exist, not change
func (mp *metaPartition) getDentry(dentry *Dentry) (status uint8) {
	status = proto.OpOk
	item := mp.dentryTree.Get(dentry)
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
func (mp *metaPartition) getInode(ino *Inode) (status uint8) {
	status = proto.OpOk
	item := mp.inodeTree.Get(ino)
	if item == nil {
		status = proto.OpNotExistErr
		return
	}
	ino = item.(*Inode)
	return
}

func (mp *metaPartition) getInodeTree() *btree.BTree {
	return mp.inodeTree
}

// CreateDentry insert dentry into dentry tree.
func (mp *metaPartition) createDentry(dentry *Dentry) (status uint8) {
	// TODO: Implement it.
	status = proto.OpOk
	mp.dentryMu.Lock()
	if mp.dentryTree.Has(dentry) {
		mp.dentryMu.Unlock()
		status = proto.OpExistErr
		return
	}
	mp.dentryTree.ReplaceOrInsert(dentry)
	mp.dentryMu.Unlock()
	return
}

// DeleteDentry delete dentry from dentry tree.
func (mp *metaPartition) deleteDentry(dentry *Dentry) (status uint8) {
	// TODO: Implement it.
	status = proto.OpOk
	mp.dentryMu.Lock()
	item := mp.dentryTree.Delete(dentry)
	mp.dentryMu.Unlock()
	if item == nil {
		status = proto.OpNotExistErr
		return
	}
	dentry = item.(*Dentry)
	return
}

// CreateInode create inode to inode tree.
func (mp *metaPartition) createInode(ino *Inode) (status uint8) {
	// TODO: Implement it.
	status = proto.OpOk
	mp.inodeMu.Lock()
	if mp.inodeTree.Has(ino) {
		mp.inodeMu.Unlock()
		status = proto.OpExistErr
		return
	}
	mp.inodeTree.ReplaceOrInsert(ino)
	mp.inodeMu.Unlock()
	return
}

// DeleteInode delete specified inode item from inode tree.
func (mp *metaPartition) deleteInode(ino *Inode) (status uint8) {
	// TODO: Implement it.
	status = proto.OpOk
	mp.inodeMu.Lock()
	item := mp.inodeTree.Delete(ino)
	mp.inodeMu.Unlock()
	if item == nil {
		status = proto.OpNotExistErr
		return
	}
	ino = item.(*Inode)
	return
}

func (mp *metaPartition) openFile(ino *Inode) (status uint8) {
	item := mp.inodeTree.Get(ino)
	if item == nil {
		status = proto.OpNotExistErr
		return
	}
	item.(*Inode).AccessTime = time.Now().Unix()
	status = proto.OpOk
	return
}

func (mp *metaPartition) readDir(req *ReadDirReq) (resp *ReadDirResp) {
	begDentry := &Dentry{
		ParentId: req.ParentID,
	}
	endDentry := &Dentry{
		ParentId: req.ParentID + 1,
	}
	mp.dentryTree.AscendRange(begDentry, endDentry, func(i btree.Item) bool {
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

func (mp *metaPartition) AppendExtents(ino *Inode) (status uint8) {
	exts := ino.Extents
	status = proto.OpOk
	item := mp.inodeTree.Get(ino)
	if item == nil {
		status = proto.OpNotExistErr
		return
	}
	ino = item.(*Inode)
	ino.AppendExtents(exts)
	ino.ModifyTime = time.Now().Unix()
	return
}

func (mp *metaPartition) offlinePartition() (err error) {
	return
}

func (mp *metaPartition) updatePartition(end uint64) (err error) {
	oldEnd := mp.config.End
	mp.config.End = end
	defer func() {
		if err != nil {
			mp.config.End = oldEnd
		}
	}()
	/*
	err = mp.StoreMeta()
	*/
	return
}

func (mp *metaPartition) deletePartition() (err error) {
	mp.Stop()
	/*
	mp.deletePartition(mp.ID)
	err = os.RemoveAll(mp.RootDir)
	*/
	return
}