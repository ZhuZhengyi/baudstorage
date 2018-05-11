package raftstore

import (
	"github.com/juju/errors"
	"github.com/tiglabs/raft"
	"github.com/tiglabs/raft/proto"
)

var (
	ErrNotLeader = errors.New("not leader")
)

type PartitionStatus = raft.Status

type PartitionFsm interface {
	raft.StateMachine
	Store
}

type Partition interface {
	Submit(cmd []byte) (resp interface{}, err error)
	ChangeMember(changeType proto.ConfChangeType, peer proto.Peer, context []byte) (resp interface{}, err error)
	Stop() error
	Status() (status *PartitionStatus)
	LeaderTerm() (leaderId, term uint64)
	IsLeader() bool
	AppliedIndex() uint64
}

type partition struct {
	id       uint64
	raft     *raft.RaftServer
	config   *PartitionConfig
	resolver NodeResolver
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
	isLeader = p.raft != nil && p.IsLeader()
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
