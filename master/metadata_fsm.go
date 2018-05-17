package master

import (
	"fmt"
	"github.com/tiglabs/baudstorage/raftstore"
	"github.com/tiglabs/raft"
	"github.com/tiglabs/raft/proto"
	"strconv"
)

const (
	Applied = "applied"
)

type RaftLeaderChangeHandler func(leader uint64)

type RaftPeerChangeHandler func(confChange *proto.ConfChange) (err error)

type RaftCmdApplyHandler func(cmd *Metadata) (err error)

type RaftRestoreHandler func()

type MetadataFsm struct {
	store               *raftstore.RocksDBStore
	applied             uint64
	leaderChangeHandler RaftLeaderChangeHandler
	peerChangeHandler   RaftPeerChangeHandler
	applyHandler        RaftCmdApplyHandler
	restoreHandler      RaftRestoreHandler
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

func (mf *MetadataFsm) RegisterApplyHandler(handler RaftCmdApplyHandler) {
	mf.applyHandler = handler
}
func (mf *MetadataFsm) RegisterRestoreHandler(handler RaftRestoreHandler) {
	mf.restoreHandler = handler
}

func (mf *MetadataFsm) restore() {
	mf.restoreApplied()
	mf.restoreHandler()
}

func (mf *MetadataFsm) restoreApplied() {

	value, err := mf.Get(Applied)
	if err != nil {
		panic(fmt.Sprintf("Failed to restore applied err:%v", err.Error()))
	}
	byteValues := value.([]byte)
	if len(byteValues) == 0 {
		mf.applied = 0
		return
	}
	applied, err := strconv.ParseUint(string(byteValues), 10, 64)
	if err != nil {
		panic(fmt.Sprintf("Failed to restore applied,err:%v ", err.Error()))
	}
	mf.applied = applied
}

func (mf *MetadataFsm) Apply(command []byte, index uint64) (interface{}, error) {
	var err error
	cmd := new(Metadata)
	if err = cmd.Unmarshal(command); err != nil {
		err = fmt.Errorf("action[fsmApply],unmarshal data:%v, err:%v", command, err.Error())
		return nil, err
	}
	if _, err = mf.Put(cmd.K, cmd.V); err != nil {
		return nil, err
	}
	if _, err = mf.Put(Applied, index); err != nil {
		return nil, err
	}
	if err = mf.applyHandler(cmd); err != nil {
		return nil, err
	}
	mf.applied = index
	return nil, nil
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

func (mf *MetadataFsm) ApplySnapshot(peers []proto.Peer, iterator proto.SnapIterator) error {
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
