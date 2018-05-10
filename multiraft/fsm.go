package multiraft

import (
	"github.com/tiglabs/raft"
)

type PartitionFsm interface {
	raft.StateMachine
	Store
}
