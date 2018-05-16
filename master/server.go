package master

import (
	"fmt"
	"sync"

	"github.com/tiglabs/baudstorage/util/config"
	"github.com/tiglabs/baudstorage/raftstore"
)

//config keys
const (
	HttpPort    = "httpPort"
	LogDir      = "logDir"
	RootUrlPath = "/"
)

type Master struct {
	id        uint64
	walDir    string
	config    *config.Config
	cluster   *Cluster
	raftStore raftstore.RaftStore
	wg        sync.WaitGroup
}

func (m *Master) Start(cfg *config.Config) (err error) {
	if err = m.parseConfig(cfg); err != nil {
		return
	}

	if m.raftStore,err = m.createRaftStore();err!= nil {
		return
	}
	m.startHttpService()
	return nil
}

func (m *Master) createRaftStore() (store raftstore.RaftStore, err error) {
	raftCfg := &raftstore.Config{NodeID: m.id, WalPath: m.walDir}
	store, err = raftstore.NewRaftStore(raftCfg)
	return
}

func (m *Master) parseConfig(cfg *config.Config) (err error) {
	logDir := cfg.GetString(LogDir)
	if logDir == "" {
		return fmt.Errorf("bad config file,logDir is null")
	}
	return
}

func (m *Master) Shutdown() {
	panic("implement me")
}

func (m *Master) Sync() {
	panic("implement me")
}

func NewServer() *Master {
	return &Master{}
}
