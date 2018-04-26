package raftopt

import (
	"errors"
	"fmt"
	"github.com/tiglabs/raft"
	"sync"
)

//Resolver ...
type Resolver struct {
	nodes map[uint64]struct{}
	sync.Mutex
}

//NewResolver ...
func NewResolver() *Resolver {
	return &Resolver{nodes: make(map[uint64]struct{})}
}

//AddNode ...
func (r *Resolver) AddNode(nodeID uint64) {
	r.Lock()
	defer r.Unlock()
	r.nodes[nodeID] = struct{}{}
}

//RemoveNode ...
func (r *Resolver) RemoveNode(nodeID uint64) {
	r.Lock()
	defer r.Unlock()
	delete(r.nodes, nodeID)
}

//AllNodes ...
func (r *Resolver) AllNodes() (all []uint64) {
	r.Lock()
	defer r.Unlock()
	for k := range r.nodes {
		all = append(all, k)
	}
	return
}

//NodeAddress ...
func (r *Resolver) NodeAddress(nodeID uint64, socketType raft.SocketType) (addr string, err error) {
	raftAddr, ok := AddrDatabase[nodeID]
	if !ok {
		return "", fmt.Errorf("action[NodeAddressErr],err:%v", errors.New("no such node"))
	}
	switch socketType {
	case raft.HeartBeat:
		return raftAddr.Heartbeat, nil
	case raft.Replicate:
		return raftAddr.Replicate, nil
	default:
		return "", fmt.Errorf("action[NodeAddressErr],err:%v", errors.New("unknown socket type"))
	}
}
