package metanode

import (
	"github.com/tiglabs/baudstorage/raftstore"
)

// StartRaftServer init address resolver and raftStore server instance.
func (m *MetaNode) startRaftServer() (err error) {
	//TODO: collect peers information from metaRanges
	raftConf := &raftstore.Config{
		NodeID:  m.nodeId,
		WalPath: m.raftDir,
	}
	m.raftStore, err = raftstore.NewRaftStore(raftConf)
	return
}

func (m *MetaNode) stopRaftServer() {
	if m.raftStore != nil {
		m.raftStore.Stop()
	}
}
