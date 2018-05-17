package metanode

import (
	"time"

	"github.com/google/btree"
	"github.com/tiglabs/baudstorage/proto"
	"github.com/tiglabs/baudstorage/sdk/stream"
	"os"
)

// GetDentry query dentry from DentryTree with specified dentry info;
// if it exist, the required parameter is the dentry entity,
// if not exist, not change
func (mp *MetaPartition) getDentry(dentry *Dentry) (status uint8) {
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
func (mp *MetaPartition) getInode(ino *Inode) (status uint8) {
	status = proto.OpOk
	item := mp.inodeTree.Get(ino)
	if item == nil {
		status = proto.OpNotExistErr
		return
	}
	ino = item.(*Inode)
	return
}

func (mp *MetaPartition) getInodeTree() *btree.BTree {
	return mp.inodeTree
}

// CreateDentry insert dentry into dentry tree.
func (mp *MetaPartition) createDentry(dentry *Dentry) (status uint8) {
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
func (mp *MetaPartition) deleteDentry(dentry *Dentry) (status uint8) {
	// TODO: Implement it.
	status = proto.OpOk
	mp.dentryMu.Lock()
	item := mp.dentryTree.Delete(dentry)
	mp.dentryMu.Unlock()
	if item == nil {
		status = proto.OpExistErr
		return
	}
	dentry = item.(*Dentry)
	return
}

func (mp *MetaPartition) getDentryTree() *btree.BTree {
	return mp.dentryTree
}

// CreateInode create inode to inode tree.
func (mp *MetaPartition) createInode(ino *Inode) (status uint8) {
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
func (mp *MetaPartition) deleteInode(ino *Inode) (status uint8) {
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

func (mp *MetaPartition) openFile(req *OpenReq) (status uint8) {
	item := mp.inodeTree.Get(&Inode{
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

func (mp *MetaPartition) readDir(req *ReadDirReq) (resp *ReadDirResp) {
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

func (mp *MetaPartition) putStreamKey(ino *Inode, k stream.ExtentKey) (status uint8) {
	status = proto.OpOk
	item := mp.inodeTree.Get(ino)
	if item == nil {
		status = proto.OpNotExistErr
		return
	}
	ino = item.(*Inode)
	ino.Stream.Put(k)
	return
}

func (mp *MetaPartition) offlinePartition() (err error) {

	return
}

func (mp *MetaPartition) updatePartition(start, end uint64) (err error) {
	oldStart := mp.Start
	oldEnd := mp.End
	mp.Start = start
	mp.End = end
	defer func() {
		if err != nil {
			mp.Start = oldStart
			mp.End = oldEnd
		}
	}()
	err = mp.StoreMeta()
	return
}

func (mp *MetaPartition) deletePartition() (err error) {
	mp.Stop()
	mp.MetaManager.DeleteMetaPartition(mp.ID)
	err = os.RemoveAll(mp.RootDir)
	return
}
