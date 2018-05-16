package raftstore

import (
	"encoding/json"
	"fmt"
	"strconv"
	"sync/atomic"
	"testing"

	"github.com/tiglabs/raft"
	"github.com/tiglabs/raft/proto"
)

type raftAddr struct {
	heartbeat string
	replicate string
}

type testKV struct {
	Opt uint32 `json:"op"`
	K   []byte `json:"k"`
	V   []byte `json:"v"`
}

var raftAddresses = make(map[uint64]*raftAddr)
var maxVolId uint64 = 1

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
	partitions := make(map[uint64]*partition)

	cfg.NodeID = 1
	cfg.WalPath = "wal"

	raftStore, err := NewRaftStore(&cfg)
	if err != nil {
		t.Fatal(err)
	}

	var testFsm TestFsm
	var peers []proto.Peer
	for k := range raftAddresses {
		peers = append(peers, proto.Peer{ID: k})
	}
	for i := 1; i <= 5; i++ {
		partitionCfg := &PartitionConfig{
			ID:      uint64(i),
			Applied: 0,
			SM:      &testFsm,
			Peers:   peers,
		}

		var p Partition
		p, err = raftStore.CreatePartition(partitionCfg)

		partitions[uint64(i)] = p.(*partition)

		fmt.Printf("new partition %d\n", i)

		if err != nil {
			t.Fatal(err)
		}
	}

	var (
		data []byte
	)

	kv := &testKV{Opt: 1}
	atomic.AddUint64(&maxVolId, 1)
	value := strconv.FormatUint(maxVolId, 10)
	kv.K = []byte("max_value_key")
	kv.V = []byte(value)

	if data, err = json.Marshal(kv); err != nil {
		err = fmt.Errorf("action[KvsmAllocateVolID],marshal kv:%v,err:%v", kv, err.Error())
		if err != nil {
			t.Fatal(err)
		}
	}

	fmt.Printf("==========encode kv end ===========\n")

	var raftServer *raft.RaftServer
	raftServer = partitions[1].raft
	leader := raftServer.LeaderTerm(1)
	fmt.Printf("==========leader is =============\n", leader)

	_, err = partitions[uint64(1)].Submit(data)
	if err != nil {
		t.Fatal(err)
	}

	fmt.Printf("==========submit ok===========\n")

}
