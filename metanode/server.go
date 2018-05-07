package metanode

import (
	"errors"
	"github.com/tiglabs/baudstorage/raftopt"
	"github.com/tiglabs/raft"
	"sync"

	"github.com/tiglabs/baudstorage/util/config"
	"github.com/tiglabs/baudstorage/util/log"
)

// Configuration keys
const (
	cfgNodeId  = "nodeID"
	cfgListen  = "listen"
	cfgDataDir = "dataDir"
	cfgLogDir  = "logDir"
)

// State type definition
type nodeState uint8

// State constants
const (
	sReady nodeState = iota
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
	raftResolver     *raftopt.Resolver
	raftServer       *raft.RaftServer
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
	// Load metaRanges relation from file

	// Register metaRaftEngine

	// Start raft server
	if err = m.startRaftServer(); err != nil {
		return
	}
	//start raft

	// Start tcp server
	if err = m.startTcpServer(); err != nil {
		return
	}
	// Start reply
	m.state = sRunning
	m.wg.Add(1)
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
	m.stopRaftServer()
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
	m.dataDir = cfg.GetString(cfgDataDir)
	m.logDir = cfg.GetString(cfgLogDir)
	return
}

// NewServer create an new MetaNode instance.
func NewServer() *MetaNode {
	return &MetaNode{}
}
