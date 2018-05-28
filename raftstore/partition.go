package raftstore

import (
	"github.com/juju/errors"
	"github.com/tiglabs/raft"
	"github.com/tiglabs/raft/proto"
)

// Error definitions for raft store partition.
var (
	ErrNotLeader = errors.New("not leader")
)

// PartitionStatus is a type alias of raft.Status
type PartitionStatus = raft.Status

// PartitionFsm wraps necessary methods include both FSM implementation
// and data storage operation for raft store partition.
// It extends from raft StateMachine and Store.
type PartitionFsm interface {
	raft.StateMachine
	Store
}

// Partition wraps necessary methods for raft store partition operation.
// Partition is a shard for multi-raft in RaftSore. RaftStore based on multi-raft which
// managing multiple raft replication group at same time through single
// raft server instance and system resource.
type Partition interface {
	// Submit submits command data to raft log.
	Submit(cmd []byte) (resp interface{}, err error)

	// ChaneMember submits member change event and information to raft log.
	ChangeMember(changeType proto.ConfChangeType, peer proto.Peer, context []byte) (resp interface{}, err error)

	// Stop removes this raft partition from raft server and make this partition shutdown.
	Stop() error

	// Status returns current raft status.
	Status() (status *PartitionStatus)

	// LeaderTerm returns current term of leader in raft group.
	LeaderTerm() (leaderId, term uint64)

	// IsLeader returns true if this node current is the leader in the raft group it belong to.
	IsLeader() bool

	// AppliedIndex returns current index value of applied raft log in this raft store partition.
	AppliedIndex() uint64

	// NodeManager define necessary methods for node address management.
	NodeManager
}

// This is the default implementation of Partition interface.
type partition struct {
	id       uint64
	raft     *raft.RaftServer
	config   *PartitionConfig
	resolver NodeResolver
}

func (p *partition) AddNode(nodeId uint64, addr string) {
	if p.resolver != nil {
		p.resolver.AddNode(nodeId, addr)
	}
}

func (p *partition) AddNodeWithPort(nodeId uint64, addr string, heartbeat int, replicate int) {
	if p.resolver != nil {
		p.resolver.AddNodeWithPort(nodeId, addr, heartbeat, replicate)
	}
}

func (p *partition) DeleteNode(nodeId uint64) {
	DeleteNode(p.resolver, nodeId)
}

func (p *partition) ChangeMember(changeType proto.ConfChangeType, peer proto.Peer, context []byte) (
	resp interface{}, err error) {
	if !p.IsLeader() {
		err = ErrNotLeader
		return
	}
	future := p.raft.ChangeMember(p.id, changeType, peer, context)
	resp, err = future.Response()
	return
}

func (p *partition) Stop() (err error) {
	err = p.raft.RemoveRaft(p.id)
	return
}

func (p *partition) Status() (status *PartitionStatus) {
	status = p.raft.Status(p.id)
	return
}

func (p *partition) LeaderTerm() (leaderId, term uint64) {
	leaderId, term = p.raft.LeaderTerm(p.id)
	return
}

func (p *partition) IsLeader() (isLeader bool) {
	isLeader = p.raft != nil && p.raft.IsLeader(p.id)
	return
}

func (p *partition) AppliedIndex() (applied uint64) {
	applied = p.raft.AppliedIndex(p.id)
	return
}

func (p *partition) Submit(cmd []byte) (resp interface{}, err error) {
	if !p.IsLeader() {
		err = ErrNotLeader
		return
	}
	future := p.raft.Submit(p.id, cmd)
	resp, err = future.Response()
	return
}

func newPartition(cfg *PartitionConfig, raft *raft.RaftServer, resolver NodeResolver) Partition {
	return &partition{
		id:       cfg.ID,
		raft:     raft,
		config:   cfg,
		resolver: resolver,
	}
}
