package metanode

import (
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"bytes"
	"github.com/juju/errors"
	"github.com/tiglabs/baudstorage/raftstore"
	"github.com/tiglabs/baudstorage/util"
	"github.com/tiglabs/baudstorage/util/config"
	"github.com/tiglabs/baudstorage/util/log"
)

// Configuration keys
const (
	cfgListen      = "listen"
	cfgMetaDir     = "metaDir"
	cfgRaftDir     = "raftDir"
	cfgMasterAddrs = "masterAddrs"
)

const (
	metaNodeURL = "/metaNode/add"
)

// The MetaNode manage Dentry and Inode information in multiple metaPartition, and
// through the RaftStore algorithm and other MetaNodes in the RageGroup for reliable
// data synchronization to maintain data consistency within the MetaGroup.
type MetaNode struct {
	nodeId      uint64
	listen      int
	metaDir     string //metaNode store root dir
	raftDir     string //raftStore log store base dir
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
	if err = m.register(); err != nil {
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
	m.metaDir = cfg.GetString(cfgMetaDir)
	m.raftDir = cfg.GetString(cfgRaftDir)
	addrs := cfg.GetArray(cfgMasterAddrs)
	for _, addr := range addrs {
		masterAddrs = append(masterAddrs, addr.(string))
	}
	err = m.validConfig()
	return
}

func (m *MetaNode) validConfig() (err error) {
	if m.listen <= 0 || m.listen >= 65535 {
		err = errors.Errorf("listen port: %d", m.listen)
		return
	}
	if m.metaDir == "" {
		m.metaDir = defaultMetaDir
	}
	if m.raftDir == "" {
		m.raftDir = defaultRaftDir
	}
	if len(masterAddrs) == 0 {
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

func (m *MetaNode) register() (err error) {
	for {
		m.localAddr, err = util.GetLocalIP()
		if err != nil {
			log.LogErrorf("[register]:%s", err.Error())
			continue
		}
		err = m.postNodeID()
		if err != nil {
			log.LogErrorf("[register]connect master: %s", err.Error())
			time.Sleep(3 * time.Second)
			continue
		}
		return
	}
}

func (m *MetaNode) postNodeID() (err error) {
	reqPath := fmt.Sprintf("%s?addr=%s:%d", metaNodeURL, m.localAddr, m.listen)
	msg, err := postToMaster(reqPath, nil)
	if err != nil {
		err = errors.Errorf("[postNodeID]->%s", err.Error())
		return
	}
	nodeIDStr := strings.TrimSpace(string(msg))
	if nodeIDStr == "" {
		err = errors.Errorf("[postNodeID]: master response empty body")
		return
	}
	m.nodeId, err = strconv.ParseUint(nodeIDStr, 10, 64)
	return
}

func postToMaster(reqPath string, body []byte) (msg []byte, err error) {
	var (
		req  *http.Request
		resp *http.Response
	)
	client := &http.Client{Timeout: 2 * time.Second}
	for _, maddr := range masterAddrs {
		if curMasterAddr == "" {
			curMasterAddr = maddr
		}
		reqURL := fmt.Sprintf("http://%s%s", curMasterAddr, reqPath)
		reqBody := bytes.NewBuffer(body)
		req, err = http.NewRequest("POST", reqURL, reqBody)
		if err != nil {
			log.LogErrorf("[postToMaster] construction NewRequest: %s", err.Error())
			curMasterAddr = ""
			continue
		}
		req.Header.Set("Connection", "close")
		resp, err = client.Do(req)
		if err != nil {
			log.LogErrorf("[postToMaster] connect master: %s", err.Error())
			curMasterAddr = ""
			continue
		}
		msg, err = ioutil.ReadAll(resp.Body)
		resp.Body.Close()
		if err != nil {
			log.LogErrorf("[postToMaster] read body: %s", err.Error())
			curMasterAddr = ""
			continue
		}
		if resp.StatusCode == http.StatusOK {
			return
		}
		if resp.StatusCode == http.StatusForbidden {
			curMasterAddr = strings.TrimSpace(string(msg))
			continue
		}
		curMasterAddr = ""
		err = errors.Errorf("[postToMaster] master response status_code=%d",
			resp.StatusCode)
		log.LogErrorf("[postToMaster] master response status_code=%d, "+
			"msg: %v", resp.StatusCode, string(msg))
	}
	return
}

// NewServer create an new MetaNode instance.
func NewServer() *MetaNode {
	return &MetaNode{}
}
