package metanode

import (
	"encoding/json"
	"errors"
	"fmt"
	"sync"

	"github.com/google/btree"
	"github.com/tiglabs/raft"
	raftproto "github.com/tiglabs/raft/proto"
)

const defaultBTreeDegree = 32

// MetaPartitionFsm responsible for sync data log with other meta range through Raft
// and manage dentry and inode by B-Tree in memory.
type MetaPartitionFsm struct {
	metaPartition *MetaPartition
	applyID       uint64       // for store inode/dentry max applyID
	dentryMu      sync.RWMutex // Mutex for dentry operation.
	dentryTree    *btree.BTree // B-Tree for dentry.
	inodeMu       sync.RWMutex // Mutex for inode operation.
	inodeTree     *btree.BTree // B-Tree for inode.
}

func (mf *MetaPartitionFsm) Apply(command []byte, index uint64) (resp interface{}, err error) {
	msg := &MetaPartitionSnapshot{}
	err = msg.Decode(command)
	if err != nil {
		goto end
	}
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

func (mf *MetaPartitionFsm) ApplyMemberChange(confChange *raftproto.ConfChange, index uint64) (interface{}, error) {
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

func (mf *MetaPartitionFsm) Snapshot() (raftproto.Snapshot, error) {
	appid := mf.applyID
	ino := mf.GetInodeTree()
	dentry := mf.GetDentryTree()
	snapIter := NewSnapshotIterator(appid, ino, dentry)
	return snapIter, nil
}

func (mf *MetaPartitionFsm) ApplySnapshot(peers []raftproto.Peer,
	iter raftproto.SnapIterator) error {
	newMF := NewMetaPartitionFsm(mf.metaPartition)
	for {
		data, err := iter.Next()
		if err != nil {
			return err
		}
		snap := NewMetaPartitionSnapshot(0, nil, nil)
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
	newMF.applyID = newMF.metaPartition.RaftPartition.AppliedIndex()
	*mf = *newMF
	return nil
}

func (mf *MetaPartitionFsm) HandleFatalEvent(err *raft.FatalError) {
	panic(err)
}

func (mf *MetaPartitionFsm) HandleLeaderChange(leader uint64) {
	if leader == mf.metaPartition.RaftGroupID {
		mf.metaPartition.IsLeader = true
	}
}

func (mf *MetaPartitionFsm) Put(key, val interface{}) (resp interface{}, err error) {
	snap := NewMetaPartitionSnapshot(0, nil, nil)
	snap.Op = key.(uint32)
	snap.V = val.([]byte)
	cmd, err := json.Marshal(snap)
	if err != nil {
		return
	}
	//submit raft
	resp, err = mf.metaPartition.RaftPartition.Submit(cmd)
	if err != nil {
		return
	}
	return
}

func (mf *MetaPartitionFsm) Get(key interface{}) (interface{}, error) {
	return nil, nil
}

func (mf *MetaPartitionFsm) Del(key interface{}) (interface{}, error) {
	return nil, nil
}
