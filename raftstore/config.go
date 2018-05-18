package raftstore

import (
	"github.com/tiglabs/raft/proto"
)

// Constants for network port definition.
const (
	HeartbeatPort = 9901
	ReplicatePort = 9902
)

// Config defined necessary configuration properties for raft store.
type Config struct {
	NodeID  uint64 // Identity of raft server instance.
	WalPath string // Path of WAL(Write after Log)
	IpAddr  string // IP address of node
}

// PartitionConfig defined necessary configuration properties for raft store partition.
type PartitionConfig struct {
	ID      uint64
	Applied uint64
	Leader  uint64
	Term    uint64
	Peers   []proto.Peer
	SM      PartitionFsm
}
