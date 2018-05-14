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
	for _, mr := range m.metaRangeManager.metaRangeMap {
		if err = m.createPartition(mr); err != nil {
			return
		}
	}
	return
}

func (m MetaNode) createPartition(mr *MetaRange) (err error) {
	var peers []proto.Peer
	for _, peer := range mr.Peers {
		peers = append(peers, proto.Peer{
			ID: peer.ID,
		})
	}
	partitionConf := &raftstore.PartitionConfig{
		ID:      mr.RaftGroupID,
		Applied: mr.store.applyID,
		Peers:   peers,
		SM:      mr.store,
	}
	partition, err := m.raftStore.CreatePartition(partitionConf)
	if err != nil {
		return
	}
	mr.RaftPartition = partition
	return
}
