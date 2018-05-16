package raftstore

import (
	"fmt"
	"github.com/juju/errors"
	"github.com/tiglabs/raft"
	"strings"
	"sync"
)

// Error definitions.
var (
	ErrNoSuchNode        = errors.New("no such node")
	ErrIllegalAddress    = errors.New("illegal address")
	ErrUnknownSocketType = errors.New("unknown socket type")
)

// This private struct defined necessary properties for node address info storage.
type nodeAddress struct {
	Heartbeat string
	Replicate string
}

// NodeManager defined necessary methods for node address management.
type NodeManager interface {
	// AddNode used to add node address information.
	AddNode(nodeId uint64, addr string)

	// DeleteNode used to delete node address information
	// of specified node ID from NodeManager if possible.
	DeleteNode(nodeId uint64)
}

// NodeResolver defined necessary methods for both node address resolving and management.
// It extends from SocketResolver and NodeManager.
type NodeResolver interface {
	raft.SocketResolver
	NodeManager
}

// This is the default parallel-safe implementation of NodeResolver interface.
type nodeResolver struct {
	nodeMap sync.Map
}

// NodeAddress resolve NodeID to net.Addr addresses.
// This method is necessary for SocketResolver interface implementation.
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

// AddNode adds node address information.
func (r *nodeResolver) AddNode(nodeId uint64, addr string) {
	if len(strings.TrimSpace(addr)) != 0 {
		fmt.Printf("add node %d %s\n",nodeId, addr)
		r.nodeMap.Store(nodeId, &nodeAddress{
			Heartbeat: fmt.Sprintf("%s:%d", addr, HeartbeatPort),
			Replicate: fmt.Sprintf("%s:%d", addr, ReplicatePort),
		})

		val, ok := r.nodeMap.Load(nodeId)
		if !ok {
			fmt.Printf("what happened?\n")
		}

		address, ok := val.(*nodeAddress)
		if !ok {
			fmt.Printf("address invalid\n")
		}

		fmt.Printf("h %s r %s\n", address.Heartbeat, address.Replicate)
	}
}

// DeleteNode deletes node address information of specified node ID from NodeManager if possible.
func (r *nodeResolver) DeleteNode(nodeId uint64) {
	r.nodeMap.Delete(nodeId)
}

// NewNodeResolver returns a new NodeResolver instance for node address management and resolving.
func NewNodeResolver() NodeResolver {
	return &nodeResolver{}
}

// AddNode add node address into specified NodeManger if possible.
func AddNode(manager NodeManager, nodeId uint64, addr string) {
	if manager != nil {
		fmt.Printf("add node %d %s\n", nodeId, addr)
		manager.AddNode(nodeId, addr)
	}
}

// DeleteNode delete node address data from specified NodeManager if possible.
func DeleteNode(manager NodeManager, nodeId uint64) {
	if manager != nil {
		manager.DeleteNode(nodeId)
	}
}
