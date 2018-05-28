package metanode

import (
	"fmt"
	"io/ioutil"
	"math/rand"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/juju/errors"
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
	retryCount  int
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
	err = m.validConfig()
	return
}

func (m *MetaNode) validConfig() (err error) {
	if m.listen <= 0 || m.listen >= 65535 {
		err = errors.Errorf("listen port: %d", m.listen)
		return
	}
	if m.logDir == "" {
		m.logDir = defaultLogDir
	}
	if m.metaDir == "" {
		m.metaDir = defaultMetaDir
	}
	if m.raftDir == "" {
		m.raftDir = defaultRaftDir
	}
	if m.masterAddrs == "" {
		err = errors.New("master Addrs is empty!")
		return
	}
	return
}

func (m *MetaNode) startMetaManager() (err error) {
	if _, err = os.Stat(m.metaDir); err != nil {
		if err = os.MkdirAll(m.metaDir, 0755); err != nil {
			return
		}
	}
	// Load metaManager
	conf := MetaManagerConfig{
		NodeID:    m.nodeId,
		RootDir:   m.metaDir,
		RaftStore: m.raftStore,
	}
	m.metaManager = NewMetaManager(conf)
	err = m.metaManager.Start()
	log.LogDebugf("loadMetaManager over...")
	return
}

func (m *MetaNode) stopMetaManager() {
	if m.metaManager != nil {
		m.metaManager.Stop()
	}
}

func (m *MetaNode) validNodeID() (err error) {
	rand.Seed(time.Now().Unix())
	for {
		// Register and Get NodeID
		if m.masterAddrs == "" {
			err = errors.New("masterAddrs is empty")
			return
		}
		mAddrSlice := strings.Split(m.masterAddrs, ";")
		i := rand.Intn(len(mAddrSlice))
		m.localAddr, err = util.GetLocalIP()
		if err != nil {
			return
		}
		reqParam := fmt.Sprintf("addr=%s:%d", m.localAddr, m.listen)
		masterURL := fmt.Sprintf("http://%s/%s?%s", mAddrSlice[i],
			metaNodeURL, reqParam)
		err = m.postNodeID(masterURL)
		if err != nil {
			log.LogErrorf("connect master: %s", err.Error())
			time.Sleep(3 * time.Second)
			continue
		}
		return
	}
}

func (m *MetaNode) postNodeID(reqURL string) (err error) {
	log.LogDebugf("action[postNodeID] post connect master get nodeID: url:%s", reqURL)
	client := &http.Client{Timeout: 2 * time.Second}
	req, err := http.NewRequest("POST", reqURL, nil)
	if err != nil {
		return
	}
	req.Header.Set("Connection", "close")
	resp, err := client.Do(req)
	if err != nil {
		return
	}
	defer resp.Body.Close()
	msg, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return
	}
	if resp.StatusCode == http.StatusOK {
		m.nodeId, err = strconv.ParseUint(string(msg), 10, 64)
		return
	}
	if resp.StatusCode == http.StatusForbidden {
		m.retryCount++
		if m.retryCount > 2 {
			m.retryCount = 0
			err = errors.New("retry too many")
			return
		}
		masterAddr := strings.TrimSpace(string(msg))
		if masterAddr == "" {
			m.retryCount = 0
			err = errors.New("master response emtpy addr")
			return
		}
		reqParam := fmt.Sprintf("addr=%s:%d", m.localAddr, m.listen)
		reqURL = fmt.Sprintf("http://%s/%s?%s", masterAddr, metaNodeURL,
			reqParam)
		log.LogDebugf("action[postNodeID] retry connect master url: %s", reqURL)
		return m.postNodeID(reqURL)

	}
	if resp.StatusCode != http.StatusOK {
		err = fmt.Errorf("action[PostToNode] Data send failed,url:%v, "+
			"status code:%v , response body: %v", reqURL,
			strconv.Itoa(resp.StatusCode), string(msg))
	}
	return
}

// NewServer create an new MetaNode instance.
func NewServer() *MetaNode {
	return &MetaNode{}
}
