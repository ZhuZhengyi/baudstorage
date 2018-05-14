package metanode

import (
	"encoding/binary"
	"errors"
	"fmt"
	"sync"

	"github.com/google/btree"
	"github.com/kubernetes/kubernetes/staging/src/k8s.io/apimachinery/pkg/util/json"
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

func (mf *MetaRangeFsm) Apply(command []byte, index uint64) (resp interface{}, err error) {
	msg := &MetaRangeSnapshot{}
	err = msg.Decode(command)
	if err != nil {
		goto end
	}
	//TODO
	switch msg.Op {
	case opCreateInode:
		ino := &Inode{}
		if err = json.Unmarshal(msg.V, ino); err != nil {
			goto end
		}
		resp = mf.CreateInode(ino)
	case opDeleteInode:
		ino := &Inode{}
		if err = json.Unmarshal(msg.V, ino); err != nil {
			goto end
		}
		resp = mf.DeleteInode(ino)
	case opCreateDentry:
		den := &Dentry{}
		if err = json.Unmarshal(msg.V, den); err != nil {
			goto end
		}
		resp = mf.CreateDentry(den)
	case opDeleteDentry:
		den := &Dentry{}
		if err = json.Unmarshal(msg.V, den); err != nil {
			goto end
		}
		resp = mf.DeleteDentry(den)
	case opOpen:
		req := &OpenReq{}
		if err = json.Unmarshal(msg.V, req); err != nil {
			goto end
		}
		resp = mf.OpenFile(req)
	case opReadDir:
		req := &ReadDirReq{}
		if err = json.Unmarshal(msg.V, req); err != nil {
			goto end
		}
		resp = mf.ReadDir(req)
	case opCreateMetaRange:
	}
end:
	mf.applyID = index
	return
}

func (mf *MetaRangeFsm) ApplyMemeberChange(confChange *raftproto.ConfChange, index uint64) (interface{}, error) {
	// Write Disk
	// Rename
	// Change memory state
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
	newMF := NewMetaRangeFsm(mf.metaRange)
	for {
		data, err := iter.Next()
		if err != nil {
			return err
		}
		snap := NewMetaRangeSnapshot(0, nil, nil)
		if err = snap.Decode(data); err != nil {
			return err
		}
		switch snap.Op {
		case opCreateInode:
			var ino = &Inode{}
			ino.ParseKeyBytes(snap.K)
			ino.ParseValueBytes(snap.V)
			newMF.CreateInode(ino)
		case opCreateDentry:
			dentry := &Dentry{}
			dentry.ParseKeyBytes(snap.K)
			dentry.ParseValueBytes(snap.V)
			newMF.CreateDentry(dentry)
		default:
			return errors.New(fmt.Sprintf("unknown op=%d", snap.Op))
		}
	}
	newMF.applyID = newMF.metaRange.RaftPartition.AppliedIndex()
	*mf = *newMF
	return nil
}

func (mf *MetaRangeFsm) HandleFatalEvent(err *raft.FatalError) {
	panic(err)
}

func (mf *MetaRangeFsm) HandleLeaderChange(leader uint64) {
}

func (mf *MetaRangeFsm) Put(key, val []byte) (resp interface{}, err error) {
	snap := NewMetaRangeSnapshot(0, nil, nil)
	snap.Op = binary.BigEndian.Uint32(key)
	snap.V = val
	cmd, err := json.Marshal(snap)
	if err != nil {
		return
	}
	//submit raft
	resp, err = mf.metaRange.RaftPartition.Submit(cmd)
	return
}

func (mf *MetaRangeFsm) Get(key []byte) ([]byte, error) {
	return nil, nil
}

func (mf *MetaRangeFsm) Del(key []byte) ([]byte, error) {
	return nil, nil
}
