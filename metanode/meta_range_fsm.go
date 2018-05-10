package metanode

import (
	"errors"
	"sync"

	"github.com/google/btree"
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
