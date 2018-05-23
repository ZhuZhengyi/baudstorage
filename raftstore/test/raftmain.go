package main

import (
	"fmt"
	"strconv"
	"path"
	"log"
	"flag"
	"strings"

	"github.com/tiglabs/raft"
	"github.com/tiglabs/raft/proto"
	"github.com/tiglabs/baudstorage/util/config"
	. "github.com/tiglabs/baudstorage/raftstore"
)

type testConfig struct {
	NodeId    uint64
	peers     []proto.Peer
	peerAddrs []string
}

var TestAddresses = make(map[uint64]string)

type testSM struct {
	dir string
}

type TestFsm struct {
	RocksDBStore
	testSM
}

func (*testSM) Apply(command []byte, index uint64) (interface{}, error) {
	fmt.Printf("===test raft apply index %d\n", index)
	return nil, nil
}

func (*testSM) ApplyMemberChange(confChange *proto.ConfChange, index uint64) (interface{}, error) {
	fmt.Printf("===test raft member change index %d===\n", index)
	return nil, nil
}

func (*testSM) Snapshot() (proto.Snapshot, error) {
	fmt.Printf("===test raft snapshot===\n")
	return nil, nil
}

func (*testSM) ApplySnapshot(peers []proto.Peer, iter proto.SnapIterator) error {
	fmt.Printf("===test raft apply snapshot===\n")
	return nil
}

func (*testSM) HandleFatalEvent(err *raft.FatalError) {
	fmt.Printf("===test raft fatal event===\n")
	return
}

func (*testSM) HandleLeaderChange(leader uint64) {
	fmt.Printf("===test raft leader change to %d===\n", leader)
	return
}

func (cfg *testConfig) parsePeers(peerStr string) error {
	peerArr := strings.Split(peerStr, ",")
	cfg.peerAddrs = peerArr
	for _, peerAddr := range peerArr {
		id, ip, _, err := parsePeerAddr(peerAddr)
		if err != nil {
			return err
		}
		cfg.peers = append(cfg.peers, proto.Peer{ID: id})
		TestAddresses[id] = fmt.Sprintf("%v", ip)
	}
	return nil
}

func parsePeerAddr(peerAddr string) (id uint64, ip string, port uint64, err error) {
	peerStr := strings.Split(peerAddr, ":")
	id, err = strconv.ParseUint(peerStr[0], 10, 64)
	if err != nil {
		return
	}
	port, err = strconv.ParseUint(peerStr[2], 10, 64)
	if err != nil {
		return
	}
	ip = peerStr[1]
	return
}

func main() {
	var (
		configFile = flag.String("c", "", "config file path")
		err        error
		testCfg    testConfig
		testFsm    TestFsm
		raftCfg    Config
	)

	log.Println("Hello, Multi-raft")
	flag.Parse()
	cfg := config.LoadConfigFile(*configFile)

	peerAddrs := cfg.GetString("peers")
	if err = testCfg.parsePeers(peerAddrs); err != nil {
		log.Fatal("parse peers fail", err)
		return
	}

	partitions := make(map[uint64]Partition)

	nodeId := cfg.GetString("nodeid")
	raftCfg.NodeID, _ = strconv.ParseUint(nodeId, 10, 10)
	raftCfg.WalPath = path.Join("wal", strconv.FormatUint(raftCfg.NodeID, 10))
	raftServer, err := NewRaftStore(&raftCfg)
	if err != nil {
		log.Fatal("new raft store fail", err)
		return
	}

	for id, peer := range TestAddresses {
		raftServer.AddNode(uint64(id), peer)
	}

	log.Println("================new raft store")

	for i := 1; i <= 5; i++ {
		partitionCfg := &PartitionConfig{
			ID:    uint64(i),
			SM:    &testFsm,
			Peers: testCfg.peers,
		}

		var p Partition
		p, err = raftServer.CreatePartition(partitionCfg)

		partitions[uint64(i)] = p

		log.Println("==========new partition %d\n", i)

		if err != nil {
			log.Fatal("create partition fail", err)
			return
		}
	}
}
