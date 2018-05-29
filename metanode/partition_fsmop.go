package metanode

import (
	"os"
	"time"

	"github.com/google/btree"
	"github.com/tiglabs/baudstorage/proto"
)

// GetDentry query dentry from DentryTree with specified dentry info;
func (mp *metaPartition) getDentry(dentry *Dentry) (*Dentry, uint8) {
	status := proto.OpOk
	item := mp.dentryTree.Get(dentry)
	if item == nil {
		status = proto.OpNotExistErr
		return nil, status
	}
	dentry = item.(*Dentry)
	return dentry, status
}

// GetInode query inode from InodeTree with specified inode info;
func (mp *metaPartition) getInode(ino *Inode) (*Inode, uint8) {
	status := proto.OpOk
	item := mp.inodeTree.Get(ino)
	if item == nil {
		status = proto.OpNotExistErr
		return nil, status
	}
	ino = item.(*Inode)
	return ino, status
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
func (mp *metaPartition) deleteDentry(dentry *Dentry) (*Dentry, uint8) {
	// TODO: Implement it.
	status := proto.OpOk
	mp.dentryMu.Lock()
	item := mp.dentryTree.Delete(dentry)
	mp.dentryMu.Unlock()
	if item == nil {
		status = proto.OpNotExistErr
		return nil, status
	}
	dentry = item.(*Dentry)
	return dentry, status
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
func (mp *metaPartition) deleteInode(ino *Inode) (*Inode, uint8) {
	// TODO: Implement it.
	status := proto.OpOk
	mp.inodeMu.Lock()
	item := mp.inodeTree.Delete(ino)
	mp.inodeMu.Unlock()
	if item == nil {
		status = proto.OpNotExistErr
		return nil, status
	}
	ino = item.(*Inode)
	return ino, status
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
	resp = &ReadDirResp{}
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

func (mp *metaPartition) appendExtents(ino *Inode) (status uint8) {
	exts := ino.Extents
	status = proto.OpOk
	item := mp.inodeTree.Get(ino)
	if item == nil {
		status = proto.OpNotExistErr
		return
	}
	ino = item.(*Inode)
	exts.Range(func(i int, ext proto.ExtentKey) bool {
		ino.AppendExtents(ext)
		return true
	})
	ino.ModifyTime = time.Now().Unix()
	return
}

func (mp *metaPartition) offlinePartition() (err error) {
	return
}

func (mp *metaPartition) updatePartition(end uint64) (status uint8, err error) {
	status = proto.OpOk
	oldEnd := mp.config.End
	mp.config.End = end
	defer func() {
		if err != nil {
			mp.config.End = oldEnd
			status = proto.OpDiskErr
		}
	}()
	err = mp.StoreMeta()
	return
}

func (mp *metaPartition) deletePartition() (status uint8) {
	mp.Stop()
	os.RemoveAll(mp.config.RootDir)
	status = proto.OpOk
	return
}
