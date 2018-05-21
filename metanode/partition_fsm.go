package metanode

import (
	"encoding/json"
	"fmt"
	"github.com/tiglabs/baudstorage/proto"
	"sync/atomic"

	"github.com/tiglabs/raft"
	raftproto "github.com/tiglabs/raft/proto"
)

func (mp *metaPartition) Apply(command []byte, index uint64) (resp interface{}, err error) {
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
		resp = mp.createInode(ino)
	case opDeleteInode:
		ino := &Inode{}
		if err = json.Unmarshal(msg.V, ino); err != nil {
			goto end
		}
		resp = mp.deleteInode(ino)
	case opCreateDentry:
		den := &Dentry{}
		if err = json.Unmarshal(msg.V, den); err != nil {
			goto end
		}
		resp = mp.createDentry(den)
	case opDeleteDentry:
		den := &Dentry{}
		if err = json.Unmarshal(msg.V, den); err != nil {
			goto end
		}
		resp = mp.deleteDentry(den)
	case opOpen:
		ino := &Inode{}
		if err = json.Unmarshal(msg.V, ino); err != nil {
			goto end
		}
		resp = mp.openFile(ino)
	case opUpdatePartition:
		req := &proto.UpdateMetaPartitionRequest{}
		if err = json.Unmarshal(msg.V, req); err != nil {
			goto end
		}
		err = mp.updatePartition(req.End)
	case opExtentsAdd:
		ino := &Inode{}
		if err = json.Unmarshal(msg.V, ino); err != nil {
			goto end
		}
		mp.AppendExtents(ino)
	}
end:
	mp.applyID = index
	return
}

func (mp *metaPartition) ApplyMemberChange(confChange *raftproto.ConfChange, index uint64) (interface{}, error) {
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
	mp.applyID = index
	return nil, nil
}

func (mp *metaPartition) Snapshot() (raftproto.Snapshot, error) {
	applyID := mp.applyID
	ino := mp.getInodeTree()
	dentry := mp.dentryTree
	snapIter := NewSnapshotIterator(applyID, ino, dentry)
	return snapIter, nil
}

func (mp *metaPartition) ApplySnapshot(peers []raftproto.Peer,
	iter raftproto.SnapIterator) error {
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
			mp.createInode(ino)
		case opCreateDentry:
			dentry := &Dentry{}
			dentry.ParseKeyBytes(snap.K)
			dentry.ParseValueBytes(snap.V)
			mp.createDentry(dentry)
		default:
			return fmt.Errorf("unknown op=%d", snap.Op)
		}
	}
	mp.applyID = mp.raftPartition.AppliedIndex()
	return nil
}

func (mp *metaPartition) HandleFatalEvent(err *raft.FatalError) {
	// Panic while fatal event happen.
	panic(err)
}

func (mp *metaPartition) HandleLeaderChange(leader uint64) {
	// Take atomic operation for leader changing.
	atomic.StoreUint64(&mp.leaderID, leader)
}

func (mp *metaPartition) Put(key, val interface{}) (resp interface{}, err error) {
	snap := NewMetaPartitionSnapshot(0, nil, nil)
	snap.Op = key.(uint32)
	snap.V = val.([]byte)
	cmd, err := json.Marshal(snap)
	if err != nil {
		return
	}
	//submit raft
	resp, err = mp.raftPartition.Submit(cmd)
	if err != nil {
		return
	}
	return
}

func (mp *metaPartition) Get(key interface{}) (interface{}, error) {
	return nil, nil
}

func (mp *metaPartition) Del(key interface{}) (interface{}, error) {
	return nil, nil
}
