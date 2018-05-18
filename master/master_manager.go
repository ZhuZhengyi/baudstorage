package master

import "github.com/tiglabs/raft/proto"

func (m *Master) handleLeaderChange(leader uint64) {

}

func (m *Master) handlePeerChange(confChange *proto.ConfChange) (err error) {
	return
}

func (m *Master) handleApply(command []byte) (err error) {
	return
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
