package metanode

import (
	"strings"
	"time"

	"github.com/google/btree"
	"github.com/juju/errors"
	"github.com/tiglabs/baudstorage/proto"
	"os"
)

type ResponseDentry struct {
	Status uint8
	Msg    *Dentry
}

func NewResponseDentry() *ResponseDentry {
	return &ResponseDentry{
		Msg: &Dentry{},
	}
}

type ResponseInode struct {
	Status uint8
	Msg    *Inode
}

func NewResponseInode() *ResponseInode {
	return &ResponseInode{
		Msg: NewInode(0, 0),
	}
}

// GetDentry query dentry from DentryTree with specified dentry info;
func (mp *metaPartition) getDentry(dentry *Dentry) (*Dentry, uint8) {
	status := proto.OpOk
	mp.dentryMu.RLock()
	item := mp.dentryTree.Get(dentry)
	mp.dentryMu.RUnlock()
	if item == nil {
		status = proto.OpNotExistErr
		return nil, status
	}
	dentry = item.(*Dentry)
	return dentry, status
}

// GetInode query inode from InodeTree with specified inode info;
func (mp *metaPartition) getInode(ino *Inode) (resp *ResponseInode) {
	resp = NewResponseInode()
	resp.Status = proto.OpOk
	mp.inodeMu.RLock()
	item := mp.inodeTree.Get(ino)
	mp.inodeMu.RUnlock()
	if item == nil {
		resp.Status = proto.OpNotExistErr
		return
	}
	resp.Msg = item.(*Inode)
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
func (mp *metaPartition) deleteDentry(dentry *Dentry) (resp *ResponseDentry) {
	// TODO: Implement it.
	resp = NewResponseDentry()
	resp.Status = proto.OpOk
	mp.dentryMu.Lock()
	item := mp.dentryTree.Delete(dentry)
	mp.dentryMu.Unlock()
	if item == nil {
		resp.Status = proto.OpNotExistErr
		return
	}
	resp.Msg = item.(*Dentry)
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
func (mp *metaPartition) deleteInode(ino *Inode) (resp *ResponseInode) {
	// TODO: Implement it.
	resp = NewResponseInode()
	resp.Status = proto.OpOk
	mp.inodeMu.Lock()
	item := mp.inodeTree.Delete(ino)
	mp.inodeMu.Unlock()
	if item == nil {
		resp.Status = proto.OpNotExistErr
		return
	}
	resp.Msg = item.(*Inode)
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
	ino.Generation++
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
	return
}

func (mp *metaPartition) confAddNode(req *proto.
	MetaPartitionOfflineRequest, index uint64) (err error) {
	findAddPeer := false
	for _, peer := range mp.config.Peers {
		if peer.ID == req.AddPeer.ID {
			findAddPeer = true
			break
		}
	}
	if findAddPeer {
		mp.applyID = index
		return
	}
	mp.config.Peers = append(mp.config.Peers, req.AddPeer)
	// Write Disk
	if err = mp.storeMeta(); err != nil {
		err = errors.Errorf("[applyAddNode]->%s", err.Error())
		mp.config.Peers = mp.config.Peers[:len(mp.config.Peers)-1]
		return
	}
	addr := strings.Split(req.AddPeer.Addr, ":")[0]
	mp.raftPartition.AddNode(req.AddPeer.ID, addr)
	mp.applyID = index
	return
}

func (mp *metaPartition) confRemoveNode(req *proto.
	MetaPartitionOfflineRequest, index uint64) (err error) {
	fondRemovePeer := false
	peerIndex := -1
	for i, peer := range mp.config.Peers {
		if peer.ID == req.RemovePeer.ID {
			fondRemovePeer = true
			peerIndex = i
			break
		}
	}
	if !fondRemovePeer {
		mp.applyID = index
		return
	}
	if req.RemovePeer.ID == mp.config.NodeId {
		mp.applyID = index
		mp.Stop()
		os.RemoveAll(mp.config.RootDir)
		return
	}
	mp.config.Peers = append(mp.config.Peers[:peerIndex], mp.config.Peers[peerIndex+1:]...)
	if err = mp.storeMeta(); err != nil {
		err = errors.Errorf("[confRemoveNode] storeMeta: %s", err.Error())
		mp.config.Peers = append(mp.config.Peers, req.RemovePeer)
		return
	}
	mp.applyID = index
	return
}
