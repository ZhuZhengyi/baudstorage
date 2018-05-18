package master

import "github.com/tiglabs/raft/proto"

type LeaderInfo struct {
	addr string //host:port
}

func (m *Master) handleLeaderChange(leader uint64) {
	//todo get master addr
	//m.leaderInfo.addr =
	m.cluster.checkDataNodeHeartbeat()
	m.cluster.checkDataNodeHeartbeat()
}

func (m *Master) handlePeerChange(confChange *proto.ConfChange) (err error) {
	return
}

func (m *Master) handleApply(cmd *Metadata) (err error) {
	return m.cluster.handleApply(cmd)
}

func (m *Master) handleRestore() {
	m.cluster.idAlloc.restore()
}

func (m *Master) loadMetadata() {
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
	if err := m.cluster.loadVolGroups(); err != nil {
		panic(err)
	}

}
