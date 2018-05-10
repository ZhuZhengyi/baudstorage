package multiraft

import "github.com/tiglabs/raft/proto"

type PartitionSnapshot interface {
	proto.Snapshot
}
