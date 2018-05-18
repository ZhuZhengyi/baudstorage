package metanode

import (
	"encoding/json"
	"errors"
	"fmt"

	"github.com/tiglabs/baudstorage/proto"
	"github.com/tiglabs/raft"
	raftproto "github.com/tiglabs/raft/proto"
)

func (mp *MetaPartition) Apply(command []byte, index uint64) (resp interface{}, err error) {
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
		req := &OpenReq{}
		if err = json.Unmarshal(msg.V, req); err != nil {
			goto end
		}
		resp = mp.openFile(req)
	case opReadDir:
		req := &ReadDirReq{}
		if err = json.Unmarshal(msg.V, req); err != nil {
			goto end
		}
		resp = mp.readDir(req)
	case opUpdatePartition:
		req := &proto.UpdateMetaPartitionRequest{}
		if err = json.Unmarshal(msg.V, req); err != nil {
			goto end
		}
		err = mp.updatePartition(req.End)
	case opDeletePartition:
		mp.deletePartition()
	}
end:
	mp.applyID = index
	return
}

func (mp *MetaPartition) ApplyMemberChange(confChange *raftproto.ConfChange, index uint64) (interface{}, error) {
	var err error
	peer := &proto.Peer{}
	if err = json.Unmarshal(confChange.Context, peer); err != nil {
		return nil, err
	}

	// Change memory state
	switch confChange.Type {
	case raftproto.ConfAddNode:
		mp.Peers = append(mp.Peers, *peer)
	case raftproto.ConfRemoveNode:
		for i, p := range mp.Peers {
			if p.ID == peer.ID {
				mp.Peers = append(mp.Peers[:i], mp.Peers[i+1:]...)
			}
		}
		// TODO:
		// mp.Stop()
		mp.MetaManager.DeleteMetaPartition(mp.ID)
	case raftproto.ConfUpdateNode:
		//TODO
	}
	// Write Disk
	if err = mp.StoreMeta(); err != nil {
		return nil, err
	}
	mp.applyID = index
	return nil, err
}

func (mp *MetaPartition) Snapshot() (raftproto.Snapshot, error) {
	appid := mp.applyID
	ino := mp.getInodeTree()
	dentry := mp.getDentryTree()
	snapIter := NewSnapshotIterator(appid, ino, dentry)
	return snapIter, nil
}

func (mp *MetaPartition) ApplySnapshot(peers []raftproto.Peer,
	iter raftproto.SnapIterator) error {
	newMP := NewMetaPartition(mp.MetaPartitionConfig)
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
			newMP.createInode(ino)
		case opCreateDentry:
			dentry := &Dentry{}
			dentry.ParseKeyBytes(snap.K)
			dentry.ParseValueBytes(snap.V)
			newMP.createDentry(dentry)
		default:
			return errors.New(fmt.Sprintf("unknown op=%d", snap.Op))
		}
	}
	newMP.applyID = newMP.RaftPartition.AppliedIndex()
	*mp = *newMP
	return nil
}

func (mp *MetaPartition) HandleFatalEvent(err *raft.FatalError) {
	panic(err)
}

func (mp *MetaPartition) HandleLeaderChange(leader uint64) {
	mp.LeaderID = leader
}

func (mp *MetaPartition) Put(key, val interface{}) (resp interface{}, err error) {
	snap := NewMetaPartitionSnapshot(0, nil, nil)
	snap.Op = key.(uint32)
	if val != nil {
		snap.V = val.([]byte)
	}
	cmd, err := json.Marshal(snap)
	if err != nil {
		return
	}
	//submit raft
	resp, err = mp.RaftPartition.Submit(cmd)
	if err != nil {
		return
	}
	return
}

func (mp *MetaPartition) Get(key interface{}) (interface{}, error) {
	return nil, nil
}

func (mp *MetaPartition) Del(key interface{}) (interface{}, error) {
	return nil, nil
}
