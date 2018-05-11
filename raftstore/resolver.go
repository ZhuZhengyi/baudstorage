package raftstore

import (
	"fmt"
	"github.com/juju/errors"
	"github.com/tiglabs/raft"
	"strings"
	"sync"
)

var (
	ErrNoSuchNode        = errors.New("no such node")
	ErrIllegalAddress    = errors.New("illegal address")
	ErrUnknownSocketType = errors.New("unknown socket type")
)

type nodeAddress struct {
	Heartbeat string
	Replicate string
}

type NodeResolver interface {
	raft.SocketResolver
	AddNode(nodeId uint64, addr string)
	DeleteNode(nodeId uint64)
}

type nodeResolver struct {
	nodeMap sync.Map
}

func (r *nodeResolver) NodeAddress(nodeID uint64, stype raft.SocketType) (addr string, err error) {
	val, ok := r.nodeMap.Load(nodeID)
	if !ok {
		err = ErrNoSuchNode
		return
	}
	address, ok := val.(*nodeAddress)
	if !ok {
		err = ErrIllegalAddress
		return
	}
	switch stype {
	case raft.HeartBeat:
		addr = address.Heartbeat
	case raft.Replicate:
		addr = address.Replicate
	default:
		err = ErrUnknownSocketType
	}
	return
}

func (r *nodeResolver) AddNode(nodeId uint64, addr string) {
	if len(strings.TrimSpace(addr)) != 0 {
		r.nodeMap.Store(nodeId, &nodeAddress{
			Heartbeat: fmt.Sprintf("%s:%d", addr, HeartbeatPort),
			Replicate: fmt.Sprintf("%s:%d", addr, ReplicatePort),
		})
	}
}

func (r *nodeResolver) DeleteNode(nodeId uint64) {
	r.nodeMap.Delete(nodeId)
}

func NewNodeResolver() NodeResolver {
	return &nodeResolver{}
}
