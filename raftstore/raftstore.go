package raftstore

import (
	"github.com/tiglabs/raft"
	"github.com/tiglabs/raft/storage/wal"
	"os"
	"path"
	"strconv"
	"fmt"
)

type RaftStore interface {
	CreatePartition(cfg *PartitionConfig) (Partition, error)
	Stop()
	NodeManager
}

type raftStore struct {
	nodeId     uint64
	resolver   NodeResolver
	raftConfig *raft.Config
	raftServer *raft.RaftServer
	walPath    string
}

func (s *raftStore) AddNode(nodeId uint64, addr string) {
	AddNode(s.resolver, nodeId, addr)
}

func (s *raftStore) DeleteNode(nodeId uint64) {
	DeleteNode(s.resolver, nodeId)
}

func (s *raftStore) Stop() {
	if s.raftServer != nil {
		s.raftServer.Stop()
	}
}

func NewRaftStore(cfg *Config) (mr RaftStore, err error) {
	if err = os.MkdirAll(cfg.WalPath, os.ModeDir); err != nil {
		return
	}
	resolver := NewNodeResolver()
	rc := raft.DefaultConfig()
	rc.NodeID = cfg.NodeID
	rc.LeaseCheck = true
	rc.HeartbeatAddr = fmt.Sprintf("%s:%d", cfg.IpAddr, HeartbeatPort)
	rc.ReplicateAddr = fmt.Sprintf("%s:%d", cfg.IpAddr, ReplicatePort)
	rc.Resolver = resolver
	rs, err := raft.NewRaftServer(rc)
	if err != nil {
		return
	}
	mr = &raftStore{
		nodeId:     cfg.NodeID,
		resolver:   resolver,
		raftConfig: rc,
		raftServer: rs,
	}
	return
}

func (s *raftStore) CreatePartition(cfg *PartitionConfig) (p Partition, err error) {
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
		Leader:       cfg.Leader,
		Term:         cfg.Term,
		Storage:      ws,
		StateMachine: cfg.SM,
		Applied:      cfg.Applied,
	}
	err = s.raftServer.CreateRaft(rc)
	return
}
