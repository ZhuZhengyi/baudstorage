package raftstore

import (
	"github.com/tiglabs/raft/proto"
)

const (
	HeartbeatPort = 9901
	ReplicatePort = 9902
)

type Config struct {
	NodeID  uint64 // Identity of this node.
	WalPath string // Path of WAL(Write after log)
}

type PartitionConfig struct {
	ID       uint64
	Applied  uint64
	Peers    []proto.Peer
	RaftPath string
	SM       PartitionFsm
}
