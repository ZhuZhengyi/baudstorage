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
	ip        string
}

type testKV struct {
	Opt uint32 `json:"op"`
	K   []byte `json:"k"`
	V   []byte `json:"v"`
}

var TestAddresses = make(map[uint64]*raftAddr)
var maxVolId uint64 = 1

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

	var (
		cfg     Config
		err     error
		testFsm TestFsm
		peers   []proto.Peer
		data    []byte
	)

	for nid := 1; nid <= 3; nid++ {
		TestAddresses[uint64(nid)] = &raftAddr{
			ip:        fmt.Sprintf("172.0.0.%d", nid),
		}

		fmt.Println(TestAddresses[uint64(nid)])
	}

	raftServers := make(map[uint64]*raftStore)
	partitions := make(map[uint64]*partition)

	for n := 1; n <= 3; n++ {
		cfg.NodeID = uint64(n)
		cfg.WalPath = fmt.Sprintf("wal%d", n)

		raftServer, err := NewRaftStore(&cfg)
		if err != nil {
			t.Fatal(err)
		}

		raftServers[uint64(n)] = raftServer.(*raftStore)

		peers = append(peers, proto.Peer{ID: uint64(n)})

		for k, v := range TestAddresses{
			raftServer.AddNode(uint64(k), v.ip)
		}

		fmt.Printf("================new raft store %d\n", n)

		for i := 1; i <= 5; i++ {
			partitionCfg := &PartitionConfig{
				ID:      uint64(i),
				Applied: 0,
				Leader:  3,
				Term:    10,
				SM:      &testFsm,
				Peers:   peers,
			}

			var p Partition
			p, err = raftServer.CreatePartition(partitionCfg)

			partitions[uint64(i)] = p.(*partition)

			fmt.Printf("==========new partition %d\n", i)

			if err != nil {
				t.Fatal(err)
			}
		}
	}

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

	for k := range raftServers {
		fmt.Printf("==raftServer %d==nodeid %d==\n", k, raftServers[k].nodeId)

		for kp := range partitions {
			leader, term := partitions[kp].LeaderTerm()

			fmt.Printf("==partition %d==leader %d term %d==\n", kp, leader, term)
			isLeader := partitions[kp].IsLeader()
			fmt.Printf("==isLeader %t\n", isLeader)
			if partitions[kp].IsLeader() {
				fmt.Printf("==partition can submit==\n")
				_, err = partitions[kp].Submit(data)
				if err != nil {
					t.Fatal(err)
				}

				t.SkipNow()
			}
		}
	}
}
