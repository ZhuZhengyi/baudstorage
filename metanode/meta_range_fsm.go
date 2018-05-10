package metanode

import (
	"bufio"
	"encoding/binary"
	"encoding/json"
	"errors"
	"io"
	"io/ioutil"
	"os"
	"path"
	"sync"
	"time"

	"github.com/google/btree"
	"github.com/tiglabs/baudstorage/proto"
	"github.com/tiglabs/baudstorage/sdk/stream"
	"github.com/tiglabs/raft"
	raftproto "github.com/tiglabs/raft/proto"
)

const defaultBTreeDegree = 32

// MetaRangeFsm responsible for sync data log with other meta range through Raft
// and manage dentry and inode by B-Tree in memory.
type MetaRangeFsm struct {
	metaRange  *MetaRange
	applyID    uint64       // for restore inode/dentry max applyID
	dentryMu   sync.RWMutex // Mutex for dentry operation.
	dentryTree *btree.BTree // B-Tree for dentry.
	inodeMu    sync.RWMutex // Mutex for inode operation.
	inodeTree  *btree.BTree // B-Tree for inode.
}

func NewMetaRangeFsm(mr *MetaRange) *MetaRangeFsm {
	return &MetaRangeFsm{
		metaRange:  mr,
		dentryTree: btree.New(defaultBTreeDegree),
		inodeTree:  btree.New(defaultBTreeDegree),
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

func (mf *MetaRangeFsm) SetInodeTree(inoTree *btree.BTree) {
	mf.inodeMu.Lock()
	defer mf.inodeMu.Unlock()
	mf.inodeTree = inoTree
}

func (mf *MetaRangeFsm) GetInodeTree() *btree.BTree {
	mf.inodeMu.RLock()
	defer mf.inodeMu.RUnlock()
	return mf.inodeTree.Clone()
}

func (mf *MetaRangeFsm) SetDentryTree(denTree *btree.BTree) {
	mf.dentryMu.Lock()
	defer mf.dentryMu.Unlock()
	mf.dentryTree = denTree
}

func (mf *MetaRangeFsm) GetDentryTree() *btree.BTree {
	mf.dentryMu.RLock()
	defer mf.dentryMu.RUnlock()
	return mf.dentryTree.Clone()
}

// Load range inode from inode snapshot file
func (mf *MetaRangeFsm) LoadInode() (err error) {
	// Restore btree from ino file
	inoFile := path.Join(mf.metaRange.RootDir, "inode")
	fp, err := os.OpenFile(inoFile, os.O_RDONLY, 0644)
	if err != nil {
		if err == os.ErrNotExist {
			err = nil
		}
		return
	}
	defer fp.Close()
	reader := bufio.NewReader(fp)
	for {
		var (
			line []byte
			ino  = &Inode{}
		)
		line, _, err = reader.ReadLine()
		if err != nil {
			if err == io.EOF {
				err = nil
				return
			}
			return
		}

		if err = json.Unmarshal(line, ino); err != nil {
			return
		}
		if mf.CreateInode(ino) != proto.OpOk {
			err = errors.New("load inode info error!")
			return
		}
		if mf.metaRange.Cursor < ino.Inode {
			mf.metaRange.Cursor = ino.Inode
		}
	}
	return
}

// Restore range dentry from dentry snapshot file
func (mf *MetaRangeFsm) LoadDentry() (err error) {
	// Restore dentry from dentry file
	dentryFile := path.Join(mf.metaRange.RootDir, "dentry")
	fp, err := os.OpenFile(dentryFile, os.O_RDONLY, 0644)
	if err != nil {
		if err == os.ErrNotExist {
			err = nil
		}
		return
	}
	defer fp.Close()
	reader := bufio.NewReader(fp)
	for {
		var (
			line   []byte
			dentry = &Dentry{}
		)
		line, _, err = reader.ReadLine()
		if err != nil {
			if err == io.EOF {
				err = nil
				return
			}
			return
		}
		if err = json.Unmarshal(line, dentry); err != nil {
			return
		}
		if mf.CreateDentry(dentry) != proto.OpOk {
			err = errors.New("load dentry info error!")
			return
		}
	}
	return
}

func (mf *MetaRangeFsm) LoadApplyID() (err error) {
	applyIDFile := path.Join(mf.metaRange.RootDir, "applyid")
	data, err := ioutil.ReadFile(applyIDFile)
	if err != nil {
		return
	}
	if len(data) == 0 {
		err = errors.New("read applyid empty error")
		return
	}
	mf.applyID = binary.BigEndian.Uint64(data)
	return
}

func (mf *MetaRangeFsm) StoreToFile() (err error) {
	return
}

// Implement raft StateMachine interface
func (mf *MetaRangeFsm) Apply(command []byte, index uint64) (interface{}, error) {
	m := &MetaRangeSnapshot{}
	err := m.Decode(command)
	if err != nil {
		return nil, err
	}
	//TODO
	switch m.Op {
	}
	mf.applyID = index
	return nil, nil
}

func (mf *MetaRangeFsm) ApplyMemeberChange(confChange *raftproto.ConfChange,
	index uint64) (interface{}, error) {
	switch confChange.Type {
	case raftproto.ConfAddNode:
		//TODO
	case raftproto.ConfRemoveNode:
		//TODO
	case raftproto.ConfUpdateNode:
		//TODO

	}

	mf.applyID = index
	return nil, nil
}

func (mf *MetaRangeFsm) Snapshot() (raftproto.Snapshot, error) {
	appid := mf.applyID
	ino := mf.GetInodeTree()
	dentry := mf.GetDentryTree()
	snapIter := NewSnapshotIterator(appid, ino, dentry)
	return snapIter, nil
}

func (mf *MetaRangeFsm) ApplySnapshot(peers []raftproto.Peer,
	iter raftproto.SnapIterator) error {
	for {
		data, err := iter.Next()
		if err != nil {
			return err
		}
		snap := NewMetaRangeSnapshot("", "", "")
		if err = snap.Decode(data); err != nil {
			return err
		}
		switch snap.Op {
		case "inode":
			var ino = &Inode{}
			ino.ParseKey(snap.K)
			ino.ParseValue(snap.V)
			mf.CreateInode(ino)
		case "dentry":
			dentry := &Dentry{}
			dentry.ParseKey(snap.K)
			dentry.ParseValue(snap.V)
			mf.CreateDentry(dentry)
		default:
			return errors.New("unknow op=" + snap.Op)
		}
	}
	mf.applyID = mf.metaRange.RaftServer.AppliedIndex(mf.metaRange.RaftGroupID)
	return nil
}

func (mf *MetaRangeFsm) HandleFatalEvent(err *raft.FatalError) {

}

func (mf *MetaRangeFsm) HandleLeaderChange(leader uint64) {

}
