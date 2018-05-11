package metanode

import (
	"errors"
	"sync"

	"github.com/google/btree"
	"github.com/tiglabs/raft"
	raftproto "github.com/tiglabs/raft/proto"
	"strconv"
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

func (mf *MetaRangeFsm) Apply(command []byte, index uint64) (interface{}, error) {
	msg := &MetaRangeSnapshot{}
	err := msg.Decode(command)
	if err != nil {
		return nil, err
	}
	//TODO
	switch msg.Op {
	/*
		case opCreateInode:
		case opDeleteInode:
		case opCreateDentry:
		case opDeleteDentry:
	*/
	}
	mf.applyID = index
	return nil, nil
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
		snap := NewMetaRangeSnapshot(0, "", "")
		if err = snap.Decode(data); err != nil {
			return err
		}
		switch snap.Op {
		case opCreateInode:
			var ino = &Inode{}
			ino.ParseKey(snap.K)
			ino.ParseValue(snap.V)
			newMF.CreateInode(ino)
		case opCreateDentry:
			dentry := &Dentry{}
			dentry.ParseKey(snap.K)
			dentry.ParseValue(snap.V)
			newMF.CreateDentry(dentry)
		default:
			return errors.New("unknown op=" + strconv.Itoa(snap.Op))
		}
	}
	newMF.applyID = newMF.metaRange.RaftServer.AppliedIndex(newMF.metaRange.
		RaftGroupID)
	*mf = *newMF
	return nil
}

func (mf *MetaRangeFsm) HandleFatalEvent(err *raft.FatalError) {
	panic(err)
}

func (mf *MetaRangeFsm) HandleLeaderChange(leader uint64) {
	mf.metaRange.LeaderID = leader
}

func (mf *MetaRangeFsm) Set(key, val []byte) (err error) {
	//submit raft: op,k,v
	return
}

func (mf *MetaRangeFsm) Get(key []byte) (val interface{}, err error) {
	return
}

func (mf *MetaRangeFsm) Delete(key []byte) (err error) {
	return
}
