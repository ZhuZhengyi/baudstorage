package metanode

import (
	"github.com/tiglabs/raft"
	"sync"
	"time"

	"github.com/google/btree"
	"github.com/tiglabs/baudstorage/proto"
	"github.com/tiglabs/baudstorage/raftopt"
	"github.com/tiglabs/baudstorage/sdk/stream"
)

const (
	defaultBTreeDegree  = 32
	dentryDataKeyPrefix = "dentry"
	inodeDataKeyPrefix  = "inode"
	raftRole            = "MetaRange"
)

type CursorUpdateHandler func(inodeId uint64)

// MetaRangeFsmConfig wraps necessary properties for MetaRangeFsm instantiation.
type MetaRangeFsmConfig struct {
	RaftId      uint64
	RaftGroupId uint64
	MetaDataDir string
	RaftDataDir string
	Raft        *raft.RaftServer
}

// MetaRangeFsm responsible for sync data log with other meta range through Raft
// and manage dentry and inode by B-Tree in memory.
type MetaRangeFsm struct {
	metaRangeId         string                // ID of this meta range.
	metaDir             string                // Data file directory.
	metaStore           *raftopt.RocksDBStore // Repository for meta include dentry and inode.
	dentryMu            sync.RWMutex          // Mutex for dentry operation.
	dentryTree          *btree.BTree          // B-Tree for dentry.
	inodeMu             sync.RWMutex          // Mutex for inode operation.
	inodeTree           *btree.BTree          // B-Tree for inode.
	inodeStore          *raftopt.RocksDBStore // Repository for inode.
	raftStoreFsm        *raftopt.RaftStoreFsm // Raft store.
	cursorUpdateHandler CursorUpdateHandler   // Callback method for meta range cursor update.
}

func NewMetaRangeFsm(cfg MetaRangeFsmConfig) *MetaRangeFsm {
	return &MetaRangeFsm{
		metaDir:      cfg.MetaDataDir,
		metaStore:    raftopt.NewRocksDBStore(cfg.MetaDataDir),
		dentryTree:   btree.New(defaultBTreeDegree),
		inodeTree:    btree.New(defaultBTreeDegree),
		raftStoreFsm: raftopt.NewRaftStoreFsm(cfg.RaftId, cfg.RaftGroupId, raftRole, cfg.RaftDataDir, cfg.Raft),
	}
}

func (mf *MetaRangeFsm) RegisterCursorUpdateHandler(handler CursorUpdateHandler) {
	mf.cursorUpdateHandler = handler
}

// Restore load snapshot from disk and restore status.
func (mf *MetaRangeFsm) Restore() {

}

// GetDentry query dentry from DentryTree with specified dentry info;
// if it exist, the required parameter is the dentry entity,
// if not exist, not change
func (mf *MetaRangeFsm) GetDentry(dentry *Dentry) (status uint8) {
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
func (mf *MetaRangeFsm) GetInode(ino *Inode) (status uint8) {
	status = proto.OpOk
	item := mf.inodeTree.Get(ino)
	if item == nil {
		status = proto.OpNotExistErr
		return
	}
	ino = item.(*Inode)
	return
}

// CreateDentry insert dentry into dentry tree.
// Workflow:
//  Step 1. Check dentry tree.
//  Step 2. Commit to raft log with inited dentry data.
//  Step 3. Wait for raft log applied.
func (mf *MetaRangeFsm) CreateDentry(dentry *Dentry) (status uint8) {
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
// Workflow:
//  Step 1. Commit delete operation to raft log.
//  Step 2. Wait for raft log applied.
func (mf *MetaRangeFsm) DeleteDentry(dentry *Dentry) (status uint8) {
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

// CreateInode create inode to inode tree.
// Workflow:
//  Step 1. Check inode tree.
//  Step 2. Commit to raft log with inited inode data.
//  Step 3. Wait for raft log applied.
func (mf *MetaRangeFsm) CreateInode(ino *Inode) (status uint8) {
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
// Workflow:
//  Step 1. Commit delete operation to raft log.
//  Step 2. Wait for raft log applied.
func (mf *MetaRangeFsm) DeleteInode(ino *Inode) (status uint8) {
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

func (mf *MetaRangeFsm) OpenFile(req *OpenReq) (resp *OpenResp) {
	item := mf.inodeTree.Get(&Inode{
		Inode: req.Inode,
	})
	if item == nil {
		resp.Status = proto.OpNotExistErr
		return
	}
	item.(*Inode).AccessTime = time.Now().Unix()
	resp.Status = proto.OpOk
	//TODO: raft sync

	return
}

func (mf *MetaRangeFsm) ReadDir(req *ReadDirReq) (resp *ReadDirResp) {
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

func (mf *MetaRangeFsm) PutStreamKey(ino *Inode, k stream.ExtentKey) (status uint8) {
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
