package metanode

import (
	"os"

	"github.com/tiglabs/baudstorage/raftstore"
)

// StartRaftServer init address resolver and raftStore server instance.
func (m *MetaNode) startRaftServer() (err error) {
	if _, err = os.Stat(m.raftDir); err != nil {
		if err = os.MkdirAll(m.raftDir, 0755); err != nil {
			return
		}
	}
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
