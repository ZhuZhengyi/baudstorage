package master

import (
	"fmt"
	"github.com/tiglabs/raft/proto"
)

type LeaderInfo struct {
	addr string //host:port
}

func (m *Master) handleLeaderChange(leader uint64) {
	m.leaderInfo.addr = AddrDatabase[leader]
	Warn(m.clusterName, fmt.Sprintf("leader is changed to %v", m.leaderInfo.addr))
	//Once switched to the master, the checkHeartbeat is executed
	if m.id == leader {
		m.cluster.idAlloc.restore()
		m.cluster.checkDataNodeHeartbeat()
		m.cluster.checkDataNodeHeartbeat()
	}
}

func (m *Master) handlePeerChange(confChange *proto.ConfChange) (err error) {
	var msg string
	addr := string(confChange.Context)
	switch confChange.Type {
	case proto.ConfAddNode:
		m.partition.AddNode(confChange.Peer.ID, addr)
		AddrDatabase[confChange.Peer.ID] = string(confChange.Context)
		msg = fmt.Sprintf("peerID:%v,nodeAddr[%v] has been add", confChange.Peer.ID, addr)
	case proto.ConfRemoveNode:
		m.partition.DeleteNode(confChange.Peer.ID)
		msg = fmt.Sprintf("peerID:%v,nodeAddr[%v] has been removed", confChange.Peer.ID, addr)
	}
	Warn(m.clusterName, msg)
	return
}

func (m *Master) handleApply(cmd *Metadata) (err error) {
	return m.cluster.handleApply(cmd)
}

func (m *Master) handlerApplySnapshot() {
	m.cluster.namespaces = make(map[string]*NameSpace)
	m.fsm.restore()
	m.loadMetadata()
	return
}

func (m *Master) restoreIDAlloc() {
	m.cluster.idAlloc.restore()
}

// load stored meta data to memory
func (m *Master) loadMetadata() {

	m.restoreIDAlloc()
	if err := m.cluster.loadDataNodes(); err != nil {
		panic(err)
	}

	if err := m.cluster.loadMetaNodes(); err != nil {
		panic(err)
	}

	if err := m.cluster.loadNamespaces(); err != nil {
		panic(err)
	}

	if err := m.cluster.loadMetaPartitions(); err != nil {
		panic(err)
	}
	if err := m.cluster.loadDataPartitions(); err != nil {
		panic(err)
	}

}
