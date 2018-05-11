package metanode

import (
	"errors"
	"sync"

	"github.com/tiglabs/baudstorage/util/config"
	"github.com/tiglabs/baudstorage/util/log"
	"github.com/tiglabs/baudstorage/raftstore"
)

// Configuration keys
const (
	cfgNodeId  = "nodeID"
	cfgListen  = "listen"
	cfgLogDir  = "logDir"
	cfgMetaDir = "metaDir"
)

// State type definition
type nodeState uint8

// State constants
const (
	sReady   nodeState = iota
	sRunning
)

// The MetaNode manage Dentry and Inode information in multiple MetaRange, and
// through the Raft algorithm and other MetaNodes in the RageGroup for reliable
// data synchronization to maintain data consistency within the MetaGroup.
type MetaNode struct {
	nodeId           string
	listen           int
	metaDir          string //metaNode store root dir
	logDir           string
	masterAddr       string
	metaRangeManager *MetaRangeManager
	raftStore        raftstore.RaftStore
	httpStopC        chan uint8
	log              *log.Log
	state            nodeState
	stateMutex       sync.RWMutex
	wg               sync.WaitGroup
}

// Start this MeteNode with specified configuration.
//  1. Start tcp server and accept connection from master and clients.
//  2. Restore each meta range from snapshot.
//  3. Restore raft fsm of each meta range.
func (m *MetaNode) Start(cfg *config.Config) (err error) {
	// Parallel safe.
	m.stateMutex.Lock()
	defer m.stateMutex.Unlock()
	if m.state != sReady {
		// Only work if this MetaNode current is not running.
		return
	}
	// Prepare configuration
	if err = m.prepareConfig(cfg); err != nil {
		return
	}
	// Init logging
	if m.log, err = log.NewLog(m.logDir, "MetaNode", log.DebugLevel); err != nil {
		return
	}
	// Load metaRanges relation from file and start raft
	if err = m.load(); err != nil {
		return
	}

	// start raft server
	if err = m.startRaftServer(); err != nil {
		return
	}
	// Start MetaRanges Store Schedule
	if err = m.startStoreSchedule(); err != nil {
		return
	}
	// Start tcp server
	if err = m.startServer(); err != nil {
		return
	}
	// Start reply
	m.state = sRunning
	m.wg.Add(1)
	return
}

func (m *MetaNode) startStoreSchedule() (err error) {
	for _, mr := range m.metaRangeManager.metaRangeMap {
		go mr.StartStoreSchedule()
	}
	return
}

// Shutdown stop this MetaNode.
func (m *MetaNode) Shutdown() {
	// Parallel safe.
	m.stateMutex.Lock()
	defer m.stateMutex.Unlock()
	if m.state != sRunning {
		// Only work if this MetaNode current is running.
		return
	}
	// Shutdown node and release resource.
	m.stopTcpServer()
	m.state = sReady
	m.wg.Done()
}

// Sync will block invoker goroutine until this MetaNode shutdown.
func (m *MetaNode) Sync() {
	m.wg.Wait()
}

func (m *MetaNode) prepareConfig(cfg *config.Config) (err error) {
	if cfg == nil {
		err = errors.New("invalid configuration")
		return
	}
	m.nodeId = cfg.GetString(cfgNodeId)
	m.listen = int(cfg.GetInt(cfgListen))
	m.logDir = cfg.GetString(cfgLogDir)
	m.metaDir = cfg.GetString(cfgMetaDir)
	return
}

func (m *MetaNode) load() (err error) {
	// Load metaRangeManager
	err = m.metaRangeManager.LoadMetaManagers(m.metaDir)
	return
}

// NewServer create an new MetaNode instance.
func NewServer() *MetaNode {
	return &MetaNode{
		metaRangeManager: NewMetaRangeManager(),
	}
}
