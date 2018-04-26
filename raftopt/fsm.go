package raftopt

import (
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"

	pbproto "github.com/golang/protobuf/proto"
	"github.com/tecbot/gorocksdb"
	"github.com/tiglabs/raft"
	"github.com/tiglabs/raft/proto"
)

const (
	Applied          = "applied"
	Separator        = "#"
	OptApplied       = 20
	TruncateInterval = 20000
)

var NotLeader = errors.New("not leader")

type RaftLeaderChangeHandler func(leader uint64)
type RaftPeerChangeHandler func(confChange *proto.ConfChange)

//RaftStoreFsm ...
type RaftStoreFsm struct {
	role          string
	id            uint64
	applied       uint64
	groupID       uint64
	server        *raft.RaftServer
	store         *RocksDBStore
	leaderChange  RaftLeaderChangeHandler
	peerChange    RaftPeerChangeHandler
	referctStruct interface{}
}

func NewRaftStoreFsm(id, groupId uint64, role, dir string, raft *raft.RaftServer) *RaftStoreFsm {
	store := NewRocksDBStore(dir)
	return &RaftStoreFsm{
		id:      id,
		role:    role,
		server:  raft,
		groupID: groupId,
		store:   store,
	}

}

func (rs *RaftStoreFsm) Restore() {
	rs.restoreApplied()
}

func (rs *RaftStoreFsm) restoreApplied() {
	value, err := rs.store.Get(Applied)
	if err != nil {
		panic(fmt.Sprintf("Failed to restore applied err:%v", err.Error()))
	}

	if len(value) == 0 {
		rs.applied = 0
		return
	}
	applied, err := strconv.ParseUint(string(value), 10, 64)
	if err != nil {
		panic(fmt.Sprintf("Failed to restore applied,err:%v ", err.Error()))
	}
	rs.applied = applied
}

func (rs *RaftStoreFsm) GetApplied() uint64 {
	return rs.applied
}

//Apply ...
func (rs *RaftStoreFsm) Apply(data []byte, index uint64) (interface{}, error) {
	var err error
	kv := &Kv{}
	if err = pbproto.Unmarshal(data, kv); err != nil {
		err = fmt.Errorf("action[KvsmApply],unmarshal data:%v, err:%v", data, err.Error())
		return nil, err
	}
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
	return nil, nil
}

//AddNode ...
func (rs *RaftStoreFsm) AddNode(peer proto.Peer) error {
	resp := rs.server.ChangeMember(rs.groupID, proto.ConfAddNode, peer, nil)
	if _, err := resp.Response(); err != nil {
		return errors.New("action[KvsmAddNode] error: " + err.Error())
	}
	return nil
}

//RemoveNode ...
func (rs *RaftStoreFsm) RemoveNode(peer proto.Peer) error {
	resp := rs.server.ChangeMember(rs.groupID, proto.ConfRemoveNode, peer, nil)
	if _, err := resp.Response(); err != nil {
		return errors.New("action[KvsmRemoveNode] error" + err.Error())
	}
	return nil
}

//ApplyMemberChange ...
func (rs *RaftStoreFsm) ApplyMemberChange(confChange *proto.ConfChange, index uint64) (interface{}, error) {
	if rs.peerChange != nil {
		go rs.peerChange(confChange)
	}
	return nil, nil
}

//HandleLeaderChange ...
func (rs *RaftStoreFsm) HandleLeaderChange(leader uint64) {
	if rs.leaderChange != nil {
		go rs.leaderChange(leader)
	}
}

func (rs *RaftStoreFsm) RegisterLeaderChangeHandler(handler RaftLeaderChangeHandler) {
	rs.leaderChange = handler
}

func (rs *RaftStoreFsm) RegisterPeerChangeHandler(handler RaftPeerChangeHandler) {
	rs.peerChange = handler
}

//HandleFatalEvent ...
func (rs *RaftStoreFsm) HandleFatalEvent(err *raft.FatalError) {
	panic(err.Err)
}

//Snapshot ...
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

}

//ApplySnapshot ...
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

func (rs *RaftStoreFsm) GetRaftServer() *raft.RaftServer {
	return rs.server
}

type kvSnapshot struct {
	applied  uint64
	snapshot *gorocksdb.Snapshot
	rs       *RaftStoreFsm
	iterator *gorocksdb.Iterator
}

//Next ...
func (s *kvSnapshot) Next() ([]byte, error) {
	var (
		data []byte
		err  error
	)
	kv := &Kv{}

	if s.iterator.Valid() {
		key := s.iterator.Key()
		kv.K = string(key.Data())
		if strings.HasPrefix(kv.K, Applied) {
			kv.Opt = OptApplied
		}
		if strings.HasPrefix(kv.K, MaxVolIDKey) {
			kv.Opt = OptAllocateVolID
		}
		value := s.iterator.Value()
		if value != nil {
			kv.V = s.iterator.Value().Data()
		}
		if data, err = pbproto.Marshal(kv); err != nil {
			err = fmt.Errorf("action[KvsmNext],marshal kv:%v,err:%v", kv, err.Error())
			return nil, err
		}
		key.Free()
		value.Free()
		s.iterator.Next()
		return data, nil
	}
	return nil, io.EOF
}

//ApplyIndex ...
func (s *kvSnapshot) ApplyIndex() uint64 {
	return s.applied
}

//Close ...
func (s *kvSnapshot) Close() {
	s.rs.store.ReleaseSnapshot(s.snapshot)
	return
}
