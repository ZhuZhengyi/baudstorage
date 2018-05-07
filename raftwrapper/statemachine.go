package raftwrapper

import (
	"errors"
	"github.com/tiglabs/raft"
	"github.com/tiglabs/raft/proto"
	pbproto "github.com/golang/protobuf/proto"
	"fmt"
	"strconv"
	"github.com/tecbot/gorocksdb"
	"strings"
	"io"
	"encoding/json"
)

var ErrInvalidCmd = fmt.Errorf("the command is invalid")

type RaftLeaderChangeHandler func(leader uint64)
type RaftPeerChangeHandler func(confChange *proto.ConfChange) (err error)

// 实现raft.StateMachine接口
type RaftStoreFsm struct {
	id            uint64
	applied       uint64 //封装成value，类型应该怎么定义？
	maxVolId      uint32 //封装成value
	groupID       uint64    //groupID是nodeId
	server        *raft.RaftServer
	store         *RocksDBStore
	leaderChange  RaftLeaderChangeHandler
	peerChange    RaftPeerChangeHandler
}

func NewRaftStateMachine(id, groupId uint64, dir, db string, raft *raft.RaftServer) *RaftStoreFsm {
	var store *RocksDBStore

	//后续可以扩展其他db
	switch db{
	case RocksDBStorage:
		store = NewRocksDBStore(dir)

	//default is rocksdb
	default:
		store = NewRocksDBStore(dir)
	}

	return &RaftStoreFsm{
		id:      id,
		server:  raft,
		groupID: groupId,
		store:   store,
	}
}

func (rs *RaftStoreFsm) Apply(data []byte, index uint64) (interface{}, error) {
	var err error
	kv := &Kv{}
	if err = pbproto.Unmarshal(data, kv); err != nil {
		err = fmt.Errorf("action[KvsmApply],unmarshal data:%v, err:%v", data, err.Error())
		return nil, err
	}

	switch kv.Opt {
	case OptAllocateVolID:
		if _, err = rs.store.Put(kv.K, kv.V); err != nil {
			return nil, err
		}
	case OptApplied:
		kvArr := make([]*Kv, 0, 2)
		kvArr = append(kvArr, kv)
		value := strconv.FormatUint(uint64(index), 10)
		kv = &Kv{K: Applied, V: []byte(value)}
		kvArr = append(kvArr, kv)
		if _, err = rs.store.BatchPut(kvArr); err != nil {
			return nil, err
		}
		rs.applied = index
		if index > 0 && (index%TruncateInterval) == 0 {
			rs.server.Truncate(rs.groupID, index)
		}

	default:
		err = ErrInvalidCmd
		return nil, err
	}

	return nil, nil
}

func (rs *RaftStoreFsm) ApplyMemberChange(confChange *proto.ConfChange, index uint64) (interface{}, error) {
	if rs.peerChange != nil {
		go rs.peerChange(confChange)
	}
	return nil, nil
}
//将ApplyMemberChange提供给调用者？
func (rs *RaftStoreFsm) RegisterPeerChangeHandler(handler RaftPeerChangeHandler) {
	rs.peerChange = handler
}

// 获取应用数据快照
func (rs *RaftStoreFsm) Snapshot() (proto.Snapshot, error) {
	snapshot := rs.store.Snapshot()
	iterator := rs.store.Iterator(snapshot)
	iterator.SeekToFirst()
	return &kvSnapshot{
		applied:  rs.applied,
		snapshot: snapshot,
		rs:       rs,
		iterator: iterator,
	}, nil
	return nil, errors.New("not implement")
}

func (rs *RaftStoreFsm) ApplySnapshot(peers []proto.Peer, iterator proto.SnapIterator) error {
	var (
		err  error
		data []byte
	)

	kv := &Kv{}
	for err == nil {
		if data, err = iterator.Next(); err != nil {
			break
		}
		if err = pbproto.Unmarshal(data, kv); err != nil {
			err = fmt.Errorf("action[KvsmApplySnapshot],unmarshal data:%v, err:%v", data, err.Error())
			return err
		}

		if _, err = rs.store.Put(kv.K, kv.V); err != nil {
			return err
		}
		switch kv.Opt {
		case OptApplied:
			applied, err := strconv.ParseUint(string(kv.V), 10, 64)
			if err != nil {
				err = fmt.Errorf("action[KvsmApplySnapshot],parse applied err:%v", err.Error())
				return err
			}
			rs.applied = applied
		case OptAllocateVolID:
			maxVolId, err := strconv.ParseUint(string(kv.V), 10, 32)
			if err != nil {
				err = fmt.Errorf("action[KvsmApplySnapshot],parse maxVolId err:%v", err.Error())
				return err
			}
			rs.maxVolId = uint32(maxVolId)
		}
	}
	if err != nil && err != io.EOF {
		return err
	}
	return nil
}

// 来自raft的通知，该raft发生了不可恢复的故障
func (s *RaftStoreFsm) HandleFatalEvent(err *raft.FatalError) {
	panic(err.Err)
}

// 来自raft的通知，该raft发生了leader变更
// 里面不宜做耗时操作，否则会阻塞raft主流程
func (rs *RaftStoreFsm) HandleLeaderChange(leader uint64) {
	if rs.leaderChange != nil {
		go rs.leaderChange(leader)
	}
}

//对外提供的回调，处理leader change人，由调用者实现
func (rs *RaftStoreFsm) RegisterLeaderChangeHandler(handler RaftLeaderChangeHandler) {
	rs.leaderChange = handler
}

type kvSnapshot struct {
	applied  uint64
	snapshot *gorocksdb.Snapshot
	rs       *RaftStoreFsm
	iterator *gorocksdb.Iterator
}

//Next ...没有入参
// 如何封装更好？kv.Opt只能在Next方法里获取吗？那每增加一种opt都要修改？
func (ss *kvSnapshot) Next() ([]byte, error) {
	var (
		data []byte
		err  error
	)
	kv := &Kv{}

	if ss.iterator.Valid() {
		key := ss.iterator.Key()
		kv.K = string(key.Data())
		if strings.HasPrefix(kv.K, Applied) {
			kv.Opt = OptApplied
		}
		if strings.HasPrefix(kv.K, MaxVolIDKey) {
			kv.Opt = OptAllocateVolID
		}
		value := ss.iterator.Value()
		if value != nil {
			kv.V = ss.iterator.Value().Data()
		}
		if data, err = pbproto.Marshal(kv); err != nil {
			err = fmt.Errorf("action[KvsmNext],marshal kv:%v,err:%v", kv, err.Error())
			return nil, err
		}
		key.Free()
		value.Free()
		ss.iterator.Next()
		return data, nil
	}
	return nil, io.EOF
}

//ApplyIndex ...
func (ss *kvSnapshot) ApplyIndex() uint64 {
	return ss.applied
}

//Close ...
func (ss *kvSnapshot) Close() {
	ss.rs.store.ReleaseSnapshot(ss.snapshot)
	return
}

func (rsf *RaftStoreFsm) Restore() {
	//load applied and maxVolID to memory
	rsf.restoreApplied()
	rsf.restoreMaxVolID()
}

func (rsf *RaftStoreFsm) GetApplied() uint64 {
	return rsf.applied
}

func (rsf *RaftStoreFsm) restoreApplied() {

	value, err := rsf.store.Get(Applied)
	if err != nil {
		panic(fmt.Sprintf("Failed to restore applied err:%v", err.Error()))
	}

	if len(value) == 0 {
		rsf.applied = 0
		return
	}
	applied, err := strconv.ParseUint(string(value), 10, 64)
	if err != nil {
		panic(fmt.Sprintf("Failed to restore applied,err:%v ", err.Error()))
	}
	rsf.applied = applied
}

func (rsf *RaftStoreFsm) restoreMaxVolID() {
	value, err := rsf.store.Get(MaxVolIDKey)
	if err != nil {
		panic(fmt.Sprintf("Failed to restore maxVolId,err:%v ", err.Error()))
	}

	if len(value) == 0 {
		rsf.maxVolId = 0
		return
	}
	maxVolId, err := strconv.ParseUint(string(value), 10, 32)
	if err != nil {
		panic(fmt.Sprintf("Failed to restore maxVolId,err:%v ", err.Error()))
	}
	rsf.maxVolId = uint32(maxVolId)
}

func (rsf *RaftStoreFsm) buildContext(peerID uint64) (context []byte, err error) {
	address, ok := AddrDatabase[peerID]
	if !ok {
		return nil, errors.New(fmt.Sprintf("action[buildContext] err. peer[%v] not exist", peerID))
	}

	if context, err = json.Marshal(address); err != nil {
		return nil, errors.New("action[buildContext] marshal address error: " + err.Error())
	}
	return
}

//AddRaftNode...
func (rsf *RaftStoreFsm) AddRaftNode(peer proto.Peer) (err error) {
	var context []byte
	if context, err = rsf.buildContext(peer.ID); err != nil {
		return errors.New("action[KvsmAddNode] error" + err.Error())
	}
	resp := rsf.server.ChangeMember(GroupId, proto.ConfAddNode, peer, context)
	if _, err = resp.Response(); err != nil {
		return errors.New("action[smAddRaftNode] error: " + err.Error())
	}
	return nil
}

//RemoveNode ...
func (rsf *RaftStoreFsm) RemoveRaftNode(peer proto.Peer) (err error) {
	resp := rsf.server.ChangeMember(GroupId, proto.ConfRemoveNode, peer, nil)
	if _, err := resp.Response(); err != nil {
		return errors.New("action[smRemoveRaftNode] error" + err.Error())
	}
	return nil
}