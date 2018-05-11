package raftstore

import (
	"fmt"
	"github.com/tiglabs/raft"
	"github.com/tiglabs/raft/storage/wal"
	"os"
	"path"
	"strconv"
)

type MultiRaft interface {
	CreatePartition(cfg *PartitionConfig) (Partition, error)
	Stop()
}

type multiRaft struct {
	nodeId     uint64
	resolver   NodeResolver
	raftConfig *raft.Config
	raftServer *raft.RaftServer
	walPath    string
}

func (s *multiRaft) Stop() {
	if s.raftServer != nil {
		s.raftServer.Stop()
	}
}

func NewMultiRaft(cfg *Config) (mr MultiRaft, err error) {
	if err = os.MkdirAll(cfg.WalPath, os.ModeDir); err != nil {
		return
	}
	resolver := NewNodeResolver()
	rc := raft.DefaultConfig()
	rc.NodeID = cfg.NodeID
	rc.LeaseCheck = true
	rc.HeartbeatAddr = fmt.Sprintf(":%d", HeartbeatPort)
	rc.ReplicateAddr = fmt.Sprintf(":%d", ReplicatePort)
	rc.Resolver = resolver
	rs, err := raft.NewRaftServer(rc)
	if err != nil {
		return
	}
	mr = &multiRaft{
		nodeId:     cfg.NodeID,
		resolver:   resolver,
		raftConfig: rc,
		raftServer: rs,
	}
	return
}

func (s *multiRaft) CreatePartition(cfg *PartitionConfig) (p Partition, err error) {
	// Init WaL Storage for this partition.
	// Variables:
	// wc: WaL Configuration.
	// wp: WaL Path.
	// ws: WaL Storage.
	p = newPartition(cfg, s.raftServer, s.resolver)
	wc := &wal.Config{}
	wp := path.Join(s.walPath, strconv.FormatUint(cfg.ID, 10))
	ws, err := wal.NewStorage(wp, wc)
	if err != nil {
		return
	}
	rc := &raft.RaftConfig{
		ID:           cfg.ID,
		Peers:        cfg.Peers,
		Storage:      ws,
		StateMachine: cfg.SM,
		Applied:      cfg.Applied,
	}
	if err = s.raftServer.CreateRaft(rc); err != nil {
		return
	}
	return p, nil
}
