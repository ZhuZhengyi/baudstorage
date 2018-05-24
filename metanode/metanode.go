package metanode

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math/rand"
	"strings"
	"sync"
	"time"

	"github.com/juju/errors"
	"github.com/tiglabs/baudstorage/proto"
	"github.com/tiglabs/baudstorage/raftstore"
	"github.com/tiglabs/baudstorage/util"
	"github.com/tiglabs/baudstorage/util/config"
	"github.com/tiglabs/baudstorage/util/log"
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
	moduleName  = "MetaNode"
	metaNodeURL = "metaNode/add"
)

// The MetaNode manage Dentry and Inode information in multiple metaPartition, and
// through the RaftStore algorithm and other MetaNodes in the RageGroup for reliable
// data synchronization to maintain data consistency within the MetaGroup.
type MetaNode struct {
	nodeId      uint64
	listen      int
	metaDir     string //metaNode store root dir
	logDir      string
	raftDir     string //raftStore log store base dir
	masterAddrs string
	metaManager MetaManager
	localAddr   string
	raftStore   raftstore.RaftStore
	httpStopC   chan uint8
	state       ServiceState
	wg          sync.WaitGroup
}

// Start this MeteNode with specified configuration.
//  1. Start and load each meta range from snapshot.
//  2. Restore raftStore fsm of each meta range.
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
	if _, err = log.NewLog(m.logDir, moduleName, log.DebugLevel); err != nil {
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
	m.listen = int(cfg.GetFloat(cfgListen))
	m.logDir = cfg.GetString(cfgLogDir)
	m.metaDir = cfg.GetString(cfgMetaDir)
	m.raftDir = cfg.GetString(cfgRaftDir)
	m.masterAddrs = cfg.GetString(cfgMasterAddrs)
	return
}

func (m *MetaNode) startMetaManager() (err error) {
	// Load metaManager
	conf := MetaManagerConfig{
		NodeID:    m.nodeId,
		RootDir:   m.metaDir,
		RaftStore: m.raftStore,
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
	var data []byte
	for {
		// Register and Get NodeID
		if m.masterAddrs == "" {
			err = errors.New("masterAddrs is empty")
			return
		}
		mAddrSlice := strings.Split(m.masterAddrs, ";")
		rand.Seed(time.Now().Unix())
		i := rand.Intn(len(mAddrSlice))
		m.localAddr, err = util.GetLocalIP()
		if err != nil {
			return
		}
		masterURL := fmt.Sprintf("http://%s/%s", mAddrSlice[i], metaNodeURL)
		reqBody := bytes.NewBufferString(fmt.Sprintf("addr=%s:%d",
			m.localAddr, m.listen))
		data, err = util.PostToNode(reqBody.Bytes(), masterURL)
		if err != nil {
			log.LogErrorf("connect master: %s", err.Error())
			time.Sleep(3 * time.Second)
			continue
		}
		node := &proto.RegisterMetaNodeResp{}
		if err = json.Unmarshal(data, node); err != nil {
			log.LogErrorf("connect master: %s", err.Error())
			time.Sleep(3 * time.Second)
			continue
		}
		m.nodeId = node.ID
		return
	}
}

// NewServer create an new MetaNode instance.
func NewServer() *MetaNode {
	return &MetaNode{}
}
