package metanode

import (
	"encoding/json"
	"fmt"

	"github.com/juju/errors"
	"github.com/tiglabs/baudstorage/proto"
	"github.com/tiglabs/baudstorage/util/log"
	"github.com/tiglabs/raft"
	raftproto "github.com/tiglabs/raft/proto"
)

func (mp *metaPartition) Apply(command []byte, index uint64) (resp interface{}, err error) {
	defer func() {
		if err != nil {
			mp.applyID = index
		}
	}()
	msg := &MetaItem{}
	if err = msg.UnmarshalBinary(command); err != nil {
		return
	}
	switch msg.Op {
	case opCreateInode:
		ino := NewInode(0, 0)
		if err = json.Unmarshal(msg.V, ino); err != nil {
			return
		}
		resp = mp.createInode(ino)
	case opDeleteInode:
		ino := NewInode(0, 0)
		if err = json.Unmarshal(msg.V, ino); err != nil {
			return
		}
		resp = mp.deleteInode(ino)
	case opCreateDentry:
		den := &Dentry{}
		if err = json.Unmarshal(msg.V, den); err != nil {
			return
		}
		resp = mp.createDentry(den)
	case opDeleteDentry:
		den := &Dentry{}
		if err = json.Unmarshal(msg.V, den); err != nil {
			return
		}
		resp = mp.deleteDentry(den)
	case opOpen:
		ino := NewInode(0, 0)
		if err = json.Unmarshal(msg.V, ino); err != nil {
			return
		}
		resp = mp.openFile(ino)
	case opDeletePartition:
		resp = mp.deletePartition()
	case opUpdatePartition:
		req := &proto.UpdateMetaPartitionRequest{}
		if err = json.Unmarshal(msg.V, req); err != nil {
			return
		}
		resp, err = mp.updatePartition(req.End)
	case opExtentsAdd:
		ino := NewInode(0, 0)
		if err = json.Unmarshal(msg.V, ino); err != nil {
			return
		}
		resp = mp.appendExtents(ino)
	}
	return
}

func (mp *metaPartition) ApplyMemberChange(confChange *raftproto.ConfChange, index uint64) (interface{}, error) {
	var err error
	req := &proto.MetaPartitionOfflineRequest{}
	if err = json.Unmarshal(confChange.Context, req); err != nil {
		err = errors.Errorf(
			"[ApplyMemberChange]: Unmarshal MetaPartitionOfflineRequest: %s",
			err.Error())
		return nil, err
	}
	// Change memory state
	switch confChange.Type {
	case raftproto.ConfAddNode:
		err = mp.confAddNode(req, index)
	case raftproto.ConfRemoveNode:
		err = mp.confRemoveNode(req, index)
	case raftproto.ConfUpdateNode:
		//TODO
	}
	if err != nil {
		err = errors.Errorf("[ApplyMemberChange]->%s", err.Error())
	}
	return nil, err
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
		if err = snap.UnmarshalBinary(data); err != nil {
			return err
		}
		switch snap.Op {
		case opCreateInode:
			var ino = &Inode{}
			ino.UnmarshalKey(snap.K)
			ino.UnmarshalValue(snap.V)
			mp.createInode(ino)
		case opCreateDentry:
			dentry := &Dentry{}
			dentry.UnmarshalKey(snap.K)
			dentry.UnmarshalValue(snap.V)
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
	mp.leaderID = leader
	if mp.config.Start == 0 && mp.config.Cursor == 0 {
		id, _ := mp.nextInodeID()
		if mp.createInode(NewInode(id, proto.ModeDir)) != proto.OpOk {
			log.LogErrorf("[HandleLeaderChange]: create root dir inode failed!")
		}
	}
}

func (mp *metaPartition) Put(key, val interface{}) (resp interface{}, err error) {
	snap := NewMetaPartitionSnapshot(0, nil, nil)
	snap.Op = key.(uint32)
	if val != nil {
		snap.V = val.([]byte)
	}
	cmd, err := json.Marshal(snap)
	if err != nil {
		return
	}
	//submit raftStore
	resp, err = mp.raftPartition.Submit(cmd)
	return
}

func (mp *metaPartition) Get(key interface{}) (interface{}, error) {
	return nil, nil
}

func (mp *metaPartition) Del(key interface{}) (interface{}, error) {
	return nil, nil
}
