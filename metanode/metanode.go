package metanode

import (
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/tiglabs/baudstorage/raftstore"
	"github.com/tiglabs/baudstorage/util"
	"github.com/tiglabs/baudstorage/util/config"
	"github.com/tiglabs/baudstorage/util/log"
	"github.com/tiglabs/baudstorage/util/pool"
)

// Configuration keys
const (
	cfgListen     = "listen"
	cfgLogDir     = "logDir"
	cfgMetaDir    = "metaDir"
	cfgRaftDir    = "raftDir"
	cfgMasterAddr = "masterAddrs"
)

const (
	metaNodeURL = "metaNode/add"
)

// State type definition
type nodeState uint8

// State constants
const (
	sReady nodeState = iota
	sRunning
)

// The MetaNode manage Dentry and Inode information in multiple MetaPartition, and
// through the Raft algorithm and other MetaNodes in the RageGroup for reliable
// data synchronization to maintain data consistency within the MetaGroup.
type MetaNode struct {
	nodeId      uint64
	listen      int
	metaDir     string //metaNode store root dir
	logDir      string
	raftDir     string //raft log store base dir
	masterAddr  string
	masterAddrs string
	pool        *pool.ConnPool
	metaManager *MetaManager
	raftStore   raftstore.RaftStore
	httpStopC   chan uint8
	state       nodeState
	stateMutex  sync.RWMutex
	wg          sync.WaitGroup
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
	if _, err = log.NewLog(m.logDir, "MetaNode", log.DebugLevel); err != nil {
		return
	}
	// Check and Valid NodeID
	if err = m.ValidNodeID(); err != nil {
		return
	}
	// Start raft server
	if err = m.startRaftServer(); err != nil {
		return
	}
	// Load and start metaManager relation from file and start raft
	if err = m.startMetaManager(); err != nil {
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
	m.stopServer()
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
	m.listen = int(cfg.GetInt(cfgListen))
	m.logDir = cfg.GetString(cfgLogDir)
	m.metaDir = cfg.GetString(cfgMetaDir)
	m.raftDir = cfg.GetString(cfgRaftDir)
	m.masterAddrs = cfg.GetString(cfgMasterAddr)
	return
}

func (m *MetaNode) startMetaManager() (err error) {
	// Load metaManager
	err = m.metaManager.LoadMetaManagers(m.metaDir)
	// Start Raft
	m.metaManager.Range(func(id string, mp *MetaPartition) bool {
		if err = m.createPartition(mp); err != nil {
			return false
		}
		return true
	})
	return
}

func (m *MetaNode) ValidNodeID() (err error) {
	// Register and Get NodeID
	if m.masterAddrs == "" {
		err = errors.New("masterAddrs is empty")
		return
	}
	mAddrSlice := strings.Split(m.masterAddrs, ";")
	rand.Seed(time.Now().Unix())
	i := rand.Intn(len(mAddrSlice))
	var localAddr string
	conn, _ := net.DialTimeout("tcp", mAddrSlice[i], time.Second)
	defer func() {
		if conn != nil {
			conn.Close()
		}
	}()
	localAddr = strings.Split(conn.LocalAddr().String(), ":")[0]
	masterURL := fmt.Sprintf("http://%s/%s?addr=%s", mAddrSlice[i],
		metaNodeURL, fmt.Sprintf("%s:%d", localAddr, m.listen))
	data, err := util.PostToNode(nil, masterURL)
	if err != nil {
		return
	}
	node := &proto.RegisterMetaNodeResp{}
	if err = json.Unmarshal(data, node); err != nil {
		return
	}
	m.nodeId = node.ID
	return
}

// NewServer create an new MetaNode instance.
func NewServer() *MetaNode {
	return &MetaNode{
		metaManager: NewMetaRangeManager(),
		pool:        pool.NewConnPool(),
	}
}
