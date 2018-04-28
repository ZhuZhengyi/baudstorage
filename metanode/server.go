package metanode

import (
	"context"
	"errors"
	"regexp"
	"sync"

	"github.com/tiglabs/baudstorage/proto"
	"github.com/tiglabs/baudstorage/util/config"
	"github.com/tiglabs/baudstorage/util/log"
)

// Configuration keys
const (
	configKeyAddr = "address"
)

// Regular expressions
var (
	// Match 'IP:PORT'
	regexpAddr, _ = regexp.Compile(
		"^((25[0-5]|2[0-4]\\d|((1\\d{2})|([1-9]?\\d)))\\.){3}(25[0-5]|2[0-4]\\d|((1\\d{2})|([1-9]?\\d))):(\\d)+$")
)

// Errors
var (
	ErrInvalidAddress = errors.New("invalid address")
)

// State type definition
type nodeState uint8

// State constants
const (
	sReady   nodeState = iota
	sRunning
)

type MetaNode struct {
	// Configuration
	addr           string
	ip             string
	logDir         string
	masterAddr     string
	metaRangeGroup MetaRangeGroup
	// Context
	ctx           context.Context
	ctxCancelFunc context.CancelFunc
	masterReplyC  chan *proto.AdminTask
	// Runtime
	log        *log.Log
	state      nodeState
	stateMutex sync.RWMutex
	wg         sync.WaitGroup
}

// Start this MeteNode with specified configuration.
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
	// Init context
	m.initNodeContext()
	// Init logging
	if m.log, err = log.NewLog(m.logDir, "metanode", log.DebugLevel); err != nil {
		return
	}
	// Start TCP listen
	if err = m.startTcpService(); err != nil {
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
	if m.ctxCancelFunc != nil {
		m.ctxCancelFunc()
	}
	close(m.masterReplyC)

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
	addr := cfg.GetString(configKeyAddr)
	if !regexpAddr.MatchString(addr) {
		err = ErrInvalidAddress
		return
	}
	m.addr = addr
	return
}

func (m *MetaNode) initNodeContext() {
	// Init context
	m.ctx, m.ctxCancelFunc = context.WithCancel(context.Background())
	m.masterReplyC = make(chan *proto.AdminTask, 1024)
}

// NewServer create an new MetaNode instance.
func NewServer() *MetaNode {
	return &MetaNode{}
}
