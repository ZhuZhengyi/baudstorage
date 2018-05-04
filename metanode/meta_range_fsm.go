package metanode

import (
	"fmt"
	"github.com/tiglabs/baudstorage/util/log"
	"github.com/tiglabs/raft"
	"path"
	"strings"
	"sync"
	"time"

	"github.com/google/btree"
	"github.com/tiglabs/baudstorage/proto"
	"github.com/tiglabs/baudstorage/raftopt"
	"github.com/tiglabs/baudstorage/sdk/stream"
)

const (
	defaultBTreeDegree = 32
	dentryDataDirName  = "dentry"
	inodeDataDirName   = "inode"
	seqSeparator       = "*"
)

type MetaRangeFsm struct {
	// Props
	metaRangeId string
	dataPath    string
	// Runtime
	dentryMu     sync.RWMutex          // Mutex for dentry operation.
	dentryTree   *btree.BTree          // B-Tree for dentry.
	dentryStore  *raftopt.RocksDBStore // Repository for dentry.
	inodeMu      sync.RWMutex          // Mutex for inode operation.
	inodeTree    *btree.BTree          // B-Tree for inode.
	inodeStore   *raftopt.RocksDBStore // Repository for inode.
	raftStoreFsm *raftopt.RaftStoreFsm // Raft store.
}

func NewMetaRangeFsm(id, groupId uint64, role, dataPath string, raft *raft.RaftServer) *MetaRangeFsm {
	// Generate id

	// Prepare path
	dentryDataDir := path.Join(dataPath, dentryDataDirName)
	inodeDataDir := path.Join(dataPath, inodeDataDirName)
	return &MetaRangeFsm{
		dataPath:     dataPath,
		dentryTree:   btree.New(defaultBTreeDegree),
		dentryStore:  raftopt.NewRocksDBStore(dentryDataDir),
		inodeTree:    btree.New(defaultBTreeDegree),
		inodeStore:   raftopt.NewRocksDBStore(inodeDataDir),
		raftStoreFsm: raftopt.NewRaftStoreFsm(id, groupId, role, dataPath, raft),
	}
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
// Operations:
//  Step 1. Create dentry in memory.
//  Step 2. Snapshot dentry tree to disk through RocksDB.
//  Step 3. Sync with raft.
//  Step 4. Commit or rollback.
func (mf *MetaRangeFsm) CreateDentry(dentry *Dentry) (status uint8) {

	// Step 1. Create dentry in memory.
	status = proto.OpOk
	mf.dentryMu.Lock()
	if mf.dentryTree.Has(dentry) {
		mf.dentryMu.Unlock()
		status = proto.OpExistErr
		return
	}
	mf.dentryTree.ReplaceOrInsert(dentry)
	mf.dentryMu.Unlock()

	// Define rollback policy for memory.
	defer func() {
		// Rollback memory on fail.
		if status != proto.OpOk {
			mf.dentryMu.Lock()
			defer mf.dentryMu.Unlock()
			mf.dentryTree.Delete(dentry)
		}
	}()

	// Step 2. Snapshot dentry tree throught RocksDB storage.
	storeKey := strings.Join([]string{mf.metaRangeId, dentry.GetKey()}, seqSeparator)
	storeVal := []byte(dentry.GetValue())
	if _, err := mf.dentryStore.Put(storeKey, storeVal); err != nil {
		status = proto.OpErr
		return
	}

	// Define rollback policy for RocksDB storage.
	defer func() {
		// Rollback snapshot on fail.
		if status != proto.OpOk {
			if _, err := mf.dentryStore.Delete(storeKey); err != nil {
				log.LogError(fmt.Sprintf("cannot rollback snapshot from RocksDB cause %s.", err))
			}
		}
	}()

	// Step 3. Sync with raft.
	// TODO: Sync with rafe. If success then store to RocksDB.

	return
}

// DeleteDentry delete dentry from dentry tree.
// Operations:
//  Step 1. Delete dentry from memory.
//  Step 2. Snapshot dentry tree through RocksDB.
//  Step 3. Sync with raft.
//  Step 4. Commit or rollback.
func (mf *MetaRangeFsm) DeleteDentry(dentry *Dentry) (status uint8) {

	// Delete dentry from memory.
	status = proto.OpOk
	mf.dentryMu.Lock()
	item := mf.dentryTree.Delete(dentry)
	mf.dentryMu.Unlock()
	if item == nil {
		status = proto.OpExistErr
		return
	}
	dentry = item.(*Dentry)

	// Define rollback policy for memory.
	defer func() {
		if status != proto.OpOk {
			mf.dentryTree.ReplaceOrInsert(dentry)
		}
	}()

	// Step 2. Snapshot dentry tree through RocksDB storage.
	storageKey := strings.Join([]string{mf.metaRangeId, dentry.GetKey()}, seqSeparator)
	storageVal := []byte(dentry.GetValue())
	if _, err := mf.dentryStore.Delete(storageKey); err != nil {
		status = proto.OpOk
		return
	}

	// Define rollback policy for RocksDB storage.
	defer func() {
		if status != proto.OpOk {
			if _, err := mf.dentryStore.Put(storageKey, storageVal); err != nil {
				log.LogError(fmt.Sprintf("cannot rollback snapshot from RocksDB cause %s.", err))
			}
		}
	}()

	// Step 3. Sync with raft.
	//TODO: raft sync

	return
}

// CreateInode create inode to inode tree.
// Operations:
//  Step 1. Create inode and insert it to inode tree.
//  Step 2. Snapshot dentry tree through RocksDB.
//  Step 3. Sync with raft.
//  Step 4. Commit or rollback.
func (mf *MetaRangeFsm) CreateInode(ino *Inode) (status uint8) {

	// Step 1. Create inode and insert it to inode tree.
	status = proto.OpOk
	mf.inodeMu.Lock()
	if mf.inodeTree.Has(ino) {
		mf.inodeMu.Unlock()
		status = proto.OpExistErr
		return
	}
	mf.inodeTree.ReplaceOrInsert(ino)
	mf.inodeMu.Unlock()

	// Define rollback policy for memory.
	defer func() {
		if status != proto.OpOk {
			mf.inodeTree.Delete(ino)
		}
	}()

	// Step 2. Snapshot inode tree through RocksDB.
	storageKey := strings.Join([]string{mf.metaRangeId, ino.GetKey()}, seqSeparator)
	storageVal := []byte(ino.GetValue())
	if _, err := mf.inodeStore.Put(storageKey, storageVal); err != nil {
		// Error happen while snapshot inode tree.
		status = proto.OpErr
		return
	}

	// Define rollback policy for RocksDB storage.
	defer func() {
		if status != proto.OpOk {
			if _, err := mf.inodeStore.Delete(storageKey); err != nil {
				log.LogError(fmt.Sprintf("connot rollback snapshot from RocksDB cause %s.", err))
			}
		}
	}()

	// Step 3. Sync with raft.
	//TODO: raft sync

	return
}

func (mf *MetaRangeFsm) DeleteInode(ino *Inode) (status uint8) {
	status = proto.OpOk
	mf.inodeMu.Lock()
	item := mf.inodeTree.Delete(ino)
	mf.inodeMu.Unlock()
	if item == nil {
		status = proto.OpNotExistErr
		return
	}
	ino = item.(*Inode)
	//TODO: raft sync
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
