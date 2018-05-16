package metanode

import (
	"github.com/tiglabs/baudstorage/raftstore"
	"github.com/tiglabs/raft/proto"
)

// StartRaftServer init address resolver and raft server instance.
func (m *MetaNode) startRaftServer() (err error) {
	//TODO: collect peers information from metaRanges
	raftConf := &raftstore.Config{
		NodeID:  m.nodeId,
		WalPath: m.raftDir,
	}
	m.raftStore, err = raftstore.NewRaftStore(raftConf)
	if err != nil {
		return
	}
	for _, mr := range m.metaManager.partitions {
		if err = m.createPartition(mr); err != nil {
			return
		}
	}
	return
}

func (m MetaNode) createPartition(mp *MetaPartition) (err error) {
	var peers []proto.Peer
	for _, peer := range mp.Peers {
		m.raftStore.AddNode(peer.ID, peer.Addr)
	}
	partitionConf := &raftstore.PartitionConfig{
		ID:      mp.RaftGroupID,
		Applied: mp.applyID,
		Peers:   peers,
		SM:      mp,
	}
	partition, err := m.raftStore.CreatePartition(partitionConf)
	if err != nil {
		return
	}
	mp.RaftPartition = partition
	return
}
