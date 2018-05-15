package raftstore

import (
	"testing"
	"github.com/tiglabs/raft"
	"github.com/tiglabs/raft/proto"
	"fmt"
)

type raftAddr struct {
	heartbeat string
	replicate string
}

var raftAddresses = make(map[uint64]*raftAddr)

// 三个本地节点
func init() {
	for i := 1; i <= 3; i++ {
		raftAddresses[uint64(i)] = &raftAddr{
			heartbeat: fmt.Sprintf(":99%d1", i),
			replicate: fmt.Sprintf(":99%d2", i),
		}
	}
}

type testSM struct {
	dir string
}

type TestFsm struct {
	RocksDBStore
	testSM
}

func (*testSM) Apply(command []byte, index uint64) (interface{}, error) {
	return nil, nil
}

func (*testSM) ApplyMemberChange(confChange *proto.ConfChange, index uint64) (interface{}, error) {
	return nil, nil
}

func (*testSM) Snapshot() (proto.Snapshot, error) {
	return nil, nil
}

func (*testSM) ApplySnapshot(peers []proto.Peer, iter proto.SnapIterator) error {
	return nil
}

func (*testSM) HandleFatalEvent(err *raft.FatalError) {
	return
}

func (*testSM) HandleLeaderChange(leader uint64) {
	return
}


func TestRaftStore_CreateRaftStore(t *testing.T) {

	var cfg Config

	cfg.NodeID = 1
	cfg.WalPath = "wal"

	raftStore, err := NewRaftStore(&cfg)
	if err != nil{
		t.Fatal(err)
	}

	var testFsm TestFsm
	var peers []proto.Peer
	for k := range raftAddresses {
		peers = append(peers, proto.Peer{ID: k})
	}
	for i := 0; i <= 5; i++ {
		partitionCfg := &PartitionConfig{
			ID: uint64 (i),
			Applied: 0,
			SM: &testFsm,
			Peers: peers,
		}

		partition, err := raftStore.CreatePartition(partitionCfg)

		if err != nil{
			t.Fatal(err)
		}

		partition.AppliedIndex()
	}
}




