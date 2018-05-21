package metanode

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/tiglabs/baudstorage/proto"
	"github.com/tiglabs/baudstorage/raftstore"
	"github.com/tiglabs/baudstorage/util"
	"github.com/tiglabs/baudstorage/util/config"
	"github.com/tiglabs/baudstorage/util/log"
	"math/rand"
	"net"
	"strings"
	"sync"
	"time"
)

// Configuration keys
const (
	cfgListen      = "listen"
	cfgLogDir      = "logDir"
	cfgMetaDir     = "metaDir"
	cfgRaftDir     = "raftDir"
	cfgMasterAddrs = "masterAddrs"
)

const (
	metaNodeURL = "metaNode/add"
)

// The MetaNode manage Dentry and Inode information in multiple metaPartition, and
// through the Raft algorithm and other MetaNodes in the RageGroup for reliable
// data synchronization to maintain data consistency within the MetaGroup.
type MetaNode struct {
	nodeId      uint64
	listen      int
	metaDir     string //metaNode store root dir
	logDir      string
	raftDir     string //raft log store base dir
	masterAddrs string
	metaManager MetaManager
	raftStore   raftstore.RaftStore
	httpStopC   chan uint8
	state       ServiceState
	wg          sync.WaitGroup
}

// Start this MeteNode with specified configuration.
//  1. Start and load each meta range from snapshot.
//  2. Restore raft fsm of each meta range.
//  3. Start tcp server and accept connection from master and clients.
func (m *MetaNode) Start(cfg *config.Config) (err error) {
	// Parallel safe.
	if TrySwitchState(&m.state, stateReady, stateRunning) {
		defer func() {
			if err != nil {
				SetState(&m.state, stateReady)
			}
		}()
		if err = m.onStart(cfg); err != nil {
			return
		}
		m.wg.Add(1)
	}
	return
}

// Shutdown stop this MetaNode.
func (m *MetaNode) Shutdown() {
	if TrySwitchState(&m.state, stateRunning, stateReady) {
		m.onShutdown()
		m.wg.Done()
	}
}

func (m *MetaNode) onStart(cfg *config.Config) (err error) {
	if err = m.parseConfig(cfg); err != nil {
		return
	}
	if _, err = log.NewLog(m.logDir, "MetaNode", log.DebugLevel); err != nil {
		return
	}
	if err = m.validNodeID(); err != nil {
		return
	}
	if err = m.startRaftServer(); err != nil {
		return
	}
	if err = m.startMetaManager(); err != nil {
		return
	}
	if err = m.startServer(); err != nil {
		return
	}
	return
}

func (m *MetaNode) onShutdown() {
	// Shutdown node and release resource.
	m.stopServer()
	m.stopMetaManager()
	m.stopRaftServer()
}

// Sync will block invoker goroutine until this MetaNode shutdown.
func (m *MetaNode) Sync() {
	m.wg.Wait()
}

func (m *MetaNode) parseConfig(cfg *config.Config) (err error) {
	if cfg == nil {
		err = errors.New("invalid configuration")
		return
	}
	m.listen = int(cfg.GetInt(cfgListen))
	m.logDir = cfg.GetString(cfgLogDir)
	m.metaDir = cfg.GetString(cfgMetaDir)
	m.raftDir = cfg.GetString(cfgRaftDir)
	m.masterAddrs = cfg.GetString(cfgMasterAddrs)
	return
}

func (m *MetaNode) startMetaManager() (err error) {
	// Load metaManager
	conf := MetaManagerConfig{
		NodeID:  m.nodeId,
		RootDir: m.metaDir,
		Raft:    m.raftStore,
	}
	m.metaManager = NewMetaManager(conf)
	err = m.metaManager.Start()
	return
}

func (m *MetaNode) stopMetaManager() {
	if m.metaManager != nil {
		m.metaManager.Stop()
	}
}

func (m *MetaNode) validNodeID() (err error) {
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
	return &MetaNode{}
}
