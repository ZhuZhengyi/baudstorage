package master

import (
	"github.com/tiglabs/baudstorage/raftstore"
	"github.com/tiglabs/raft"
	"github.com/tiglabs/raft/proto"
)

type RaftLeaderChangeHandler func(leader uint64)

type RaftPeerChangeHandler func(confChange *proto.ConfChange) (err error)

type MetadataFsm struct {
	store               *raftstore.RocksDBStore
	leaderChangeHandler RaftLeaderChangeHandler
	peerChangeHandler   RaftPeerChangeHandler
}

func newMetadataFsm(dir string) (fsm *MetadataFsm) {
	fsm = new(MetadataFsm)
	fsm.store = raftstore.NewRocksDBStore(dir)
	return
}

func (mf *MetadataFsm) RegisterLeaderChangeHandler(handler RaftLeaderChangeHandler) {
	mf.leaderChangeHandler = handler
}

func (mf *MetadataFsm) RegisterPeerChangeHandler(handler RaftPeerChangeHandler) {
	mf.peerChangeHandler = handler
}
func (mf *MetadataFsm) Apply(command []byte, index uint64) (interface{}, error) {
	panic("implement me")
}

func (mf *MetadataFsm) ApplyMemberChange(confChange *proto.ConfChange, index uint64) (interface{}, error) {
	var err error
	if mf.peerChangeHandler != nil {
		err = mf.peerChangeHandler(confChange)
	}
	return nil, err
}

func (mf *MetadataFsm) Snapshot() (proto.Snapshot, error) {
	panic("implement me")
}

func (mf *MetadataFsm) ApplySnapshot(peers []proto.Peer, iter proto.SnapIterator) error {
	panic("implement me")
}

func (mf *MetadataFsm) HandleFatalEvent(err *raft.FatalError) {
	panic(err.Err)
}

func (mf *MetadataFsm) HandleLeaderChange(leader uint64) {
	if mf.leaderChangeHandler != nil {
		go mf.leaderChangeHandler(leader)
	}
}

func (mf *MetadataFsm) Put(key, val interface{}) (interface{}, error) {
	return mf.store.Put(key, val)
}

func (mf *MetadataFsm) Get(key interface{}) (interface{}, error) {
	return mf.store.Get(key)
}

func (mf *MetadataFsm) Del(key interface{}) (interface{}, error) {
	return mf.store.Del(key)
}
