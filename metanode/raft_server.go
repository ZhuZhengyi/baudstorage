package metanode

import (
	"github.com/tiglabs/baudstorage/raftstore"
)

// StartRaftServer init address resolver and raftStore server instance.
func (m *MetaNode) startRaftServer() (err error) {
	raftConf := &raftstore.Config{
		NodeID:  m.nodeId,
		WalPath: m.raftDir,
		IpAddr:  m.localAddr,
	}
	m.raftStore, err = raftstore.NewRaftStore(raftConf)
	return
}

func (m *MetaNode) stopRaftServer() {
	if m.raftStore != nil {
		m.raftStore.Stop()
	}
}
