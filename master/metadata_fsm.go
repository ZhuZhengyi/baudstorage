package master

import (
	"encoding/json"
	"fmt"
	"github.com/tiglabs/baudstorage/raftstore"
	"github.com/tiglabs/baudstorage/util/log"
	"github.com/tiglabs/raft"
	"github.com/tiglabs/raft/proto"
	"io"
	"strconv"
)

const (
	Applied = "applied"
)

type RaftLeaderChangeHandler func(leader uint64)

type RaftPeerChangeHandler func(confChange *proto.ConfChange) (err error)

type RaftCmdApplyHandler func(cmd *Metadata) (err error)

type MetadataFsm struct {
	store               *raftstore.RocksDBStore
	applied             uint64
	leaderChangeHandler RaftLeaderChangeHandler
	peerChangeHandler   RaftPeerChangeHandler
	applyHandler        RaftCmdApplyHandler
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

func (mf *MetadataFsm) restore() {
	mf.restoreApplied()
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

func (mf *MetadataFsm) Apply(command []byte, index uint64) (resp interface{}, err error) {
	cmd := new(Metadata)
	if err = cmd.Unmarshal(command); err != nil {
		return nil, fmt.Errorf("action[fsmApply],unmarshal data:%v, err:%v", command, err.Error())
	}
	cmdMap := make(map[string][]byte)
	cmdMap[cmd.K] = cmd.V
	cmdMap[Applied] = []byte(strconv.FormatUint(uint64(index), 10))
	switch cmd.Op {
	case OpSyncDeleteDataNode:
		if _, err = mf.Del(cmd.K); err != nil {
			return
		}
	case OpSyncDeleteMetaNode:
		if _, err = mf.Del(cmd.K); err != nil {
			return
		}
	default:
		if err = mf.BatchPut(cmdMap); err != nil {
			return
		}
	}
	if err = mf.applyHandler(cmd); err != nil {
		return
	}
	mf.applied = index
	return
}

func (mf *MetadataFsm) ApplyMemberChange(confChange *proto.ConfChange, index uint64) (interface{}, error) {
	var err error
	if mf.peerChangeHandler != nil {
		err = mf.peerChangeHandler(confChange)
	}
	return nil, err
}

func (mf *MetadataFsm) Snapshot() (proto.Snapshot, error) {
	snapshot := mf.store.RocksDBSnapshot()

	iterator := mf.store.Iterator(snapshot)
	iterator.SeekToFirst()
	return &MetadataSnapshot{
		applied:  mf.applied,
		snapshot: snapshot,
		fsm:      mf,
		iterator: iterator,
	}, nil
}

func (mf *MetadataFsm) ApplySnapshot(peers []proto.Peer, iterator proto.SnapIterator) (err error) {
	var data []byte
	for err == nil {
		if data, err = iterator.Next(); err != nil {
			goto errDeal
		}
		cmd := &Metadata{}
		if err = json.Unmarshal(data, cmd); err != nil {
			goto errDeal
		}
		if _, err = mf.store.Put(cmd.K, cmd.V); err != nil {
			goto errDeal
		}

		if err = mf.applyHandler(cmd); err != nil {
			goto errDeal
		}
	}
	return
errDeal:
	if err == io.EOF {
		return
	}
	log.LogError(fmt.Sprintf("action[ApplySnapshot] failed,err:%v", err.Error()))
	return err
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

func (mf *MetadataFsm) BatchPut(cmdMap map[string][]byte) (err error) {
	return mf.store.BatchPut(cmdMap)
}

func (mf *MetadataFsm) Get(key interface{}) (interface{}, error) {
	return mf.store.Get(key)
}

func (mf *MetadataFsm) Del(key interface{}) (interface{}, error) {
	return mf.store.Del(key)
}
