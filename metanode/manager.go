package metanode

import (
	"errors"
	"fmt"
	"github.com/tiglabs/baudstorage/proto"
	"github.com/tiglabs/baudstorage/raftstore"
	"github.com/tiglabs/baudstorage/util/pool"
	"io/ioutil"
	"net"
	"os"
	"path"
	"strings"
	"sync"
)

const metaPartitionPrefix = "partition_"

type MetaManagerConfig struct {
	DataPath string
	Raft     raftstore.RaftStore
}

// MetaManager manage all metaPartition and make mapping between namespace and metaPartition.
type MetaManager interface {
	Start() error
	Stop()
	//CreatePartition(id string, start, end uint64, peers []proto.Peer) error
	HandleMetaOperation(conn net.Conn, p *Packet) error
}

type metaManager struct {
	dataPath   string
	raft       raftstore.RaftStore
	partitions map[string]MetaPartition // Key: metaRangeId, Val: metaPartition
	masterAddr string
	connPool   *pool.ConnPool
	mu         sync.RWMutex
	state      ServiceState
}

func (m *metaManager) HandleMetaOperation(conn net.Conn, p *Packet) (err error) {
	switch p.Opcode {
	case proto.OpMetaCreateInode:
		err = m.opCreateInode(conn, p)
	case proto.OpMetaCreateDentry:
		err = m.opCreateDentry(conn, p)
	case proto.OpMetaDeleteInode:
		err = m.opDeleteInode(conn, p)
	case proto.OpMetaDeleteDentry:
		err = m.opDeleteDentry(conn, p)
	case proto.OpMetaReadDir:
		err = m.opReadDir(conn, p)
	case proto.OpMetaOpen:
		err = m.opOpen(conn, p)
	case proto.OpMetaInodeGet:
		err = m.opMetaInodeGet(conn, p)
	case proto.OpCreateMetaPartition:
		// Mater → MetaNode
		err = m.opCreateMetaPartition(conn, p)
	case proto.OpMetaNodeHeartbeat:

	default:
		err = fmt.Errorf("unknown Opcode: %d", p.Opcode)
	}
	return
}

func (m *metaManager) Start() (err error) {
	if TrySwitchState(&m.state, stateReady, stateRunning) {
		defer func() {
			if err != nil {
				SetState(&m.state, stateReady)
			}
		}()
		err = m.onStart()
	}
	return
}

func (m *metaManager) Stop() {
	if TrySwitchState(&m.state, stateRunning, stateReady) {
		m.onStop()
	}
}

func (m *metaManager) onStart() (err error) {
	m.connPool = pool.NewConnPool()
	err = m.loadPartitions()
	return
}

func (m *metaManager) onStop() {
	if m.partitions != nil {
		for _, partition := range m.partitions {
			partition.Stop()
		}
	}
	return
}

// LoadMetaPartition returns metaPartition with specified namespace if the mapping exist or report an error.
func (m *metaManager) getPartition(id string) (mp MetaPartition, err error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	mp, ok := m.partitions[id]
	if ok {
		return
	}
	err = errors.New("unknown meta partition: " + id)
	return
}

// Load meta manager snapshot from data file and restore all  meta range
// into this meta range manager.
func (m *metaManager) loadPartitions() (err error) {
	// Check metaDir directory
	fileInfo, err := os.Stat(m.dataPath)
	if err != nil {
		os.MkdirAll(m.dataPath, 0655)
		err = nil
		return
	}
	if !fileInfo.IsDir() {
		err = errors.New("metaDir must be directory")
		return
	}
	// Scan data directory.
	fileInfoList, err := ioutil.ReadDir(m.dataPath)
	if err != nil {
		return
	}
	for _, fileInfo := range fileInfoList {
		if fileInfo.IsDir() && strings.HasPrefix(fileInfo.Name(), metaPartitionPrefix) {
			go func() {
				partitionId := fileInfo.Name()[12:]
				partitionConfig := &MetaPartitionConfig{}
				partitionConfig.ID = partitionId
				partitionConfig.DataPath = path.Join(m.dataPath, fileInfo.Name())
				partition := NewMetaPartition(partitionConfig, m.raft)
				m.attachPartition(partitionId, partition)
			}()
		}
	}
	return
}

func (m *metaManager) attachPartition(id string, partition MetaPartition) (err error) {
	if err = partition.Start(); err != nil {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.partitions[id] = partition
	return
}

func (m *metaManager) detachPartition(id string) (err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if partition, has := m.partitions[id]; has {
		delete(m.partitions, id)
		partition.Stop()
	} else {
		err = fmt.Errorf("unknown partition: %d", id)
	}
	return
}

func (m *metaManager) createPartition(id string, start, end uint64, peers []proto.Peer) (err error) {
	/* Create metaPartition and add metaManager */
	mpc := &MetaPartitionConfig{
		ID:       id,
		Start:    start,
		End:      end,
		Cursor:   start,
		Peers:    peers,
		DataPath: path.Join(m.dataPath, metaPartitionPrefix+id),
	}
	partition := NewMetaPartition(mpc, m.raft)
	err = m.attachPartition(id, partition)
	return
}

func (m *metaManager) deletePartition(id string) (err error) {
	m.detachPartition(id)
	return
}

func NewMetaManager(config *MetaManagerConfig) MetaManager {
	return &metaManager{
		dataPath:   config.DataPath,
		raft:       config.Raft,
		partitions: make(map[string]MetaPartition),
	}
}
