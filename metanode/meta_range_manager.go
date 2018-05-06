package metanode

import (
	"errors"
	"github.com/tiglabs/baudstorage/util/log"
	"github.com/tiglabs/raft"
	"io/ioutil"
	"path"
	"regexp"
	"strings"
	"sync"
)

var (
	regexpMetaRangDataDirName, _ = regexp.Compile("^metarange_[\\w\\W]+_(\\d)+_(\\d)+$")
)

// MetaRangeGroup manage all MetaRange and make mapping between namespace and MetaRange.
type MetaRangeManager struct {
	dataDir      string                // Data file directory.
	raftServer   *raft.RaftServer      // Raft server instance.
	metaRangeMap map[string]*MetaRange // Key: metaRangeId, Val: metaRange
	mu           sync.RWMutex
}

// StoreMeteRange try make mapping between meta range ID and MetaRange.
func (m *MetaRangeManager) CreateMetaRange(id string, start, end uint64, peers []string) {
	if len(strings.TrimSpace(id)) > 0 {
		m.mu.Lock()
		defer m.mu.Unlock()
		if _, ok := m.metaRangeMap[id]; ok {
			return
		}
		metaRangeDataDir := path.Join(m.dataDir, "metarange_"+id)
		metaRangeCfg := MetaRangeConfig{
			RangeId:     id,
			RangeStart:  start,
			RangeEnd:    end,
			Peers:       peers,
			Raft:        m.raftServer,
			RootDataDir: metaRangeDataDir,
		}
		metaRange := NewMetaRange(metaRangeCfg)
		m.metaRangeMap[id] = metaRange
		// Start a goroutine for meta range restore operation.
		go func() {
			metaRange.UpdatePeers(peers)
			metaRange.Restore()
		}()
	}
}

// LoadMetaRange returns MetaRange with specified namespace if the mapping exist or report an error.
func (m *MetaRangeManager) LoadMetaRange(id string) (mr *MetaRange, err error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	mr, ok := m.metaRangeMap[id]
	if ok {
		return
	}
	err = errors.New("unknown meta range: " + id)
	return
}

// Restore load meta range snapshot from data file and restore all meta range
// into this meta range manager.
func (m *MetaRangeManager) Restore() {
	// Scan data directory.
	fileInfoSlice, err := ioutil.ReadDir(m.dataDir)
	if err != nil {
		log.LogError("action[MetaRangeManager.Restore],err:%v", err)
		return
	}
	for _, fileInfo, := range fileInfoSlice {
		if fileInfo.IsDir() && regexpMetaRangDataDirName.MatchString(fileInfo.Name()) {
			metaRangeId := fileInfo.Name()[10:]
			m.mu.Lock()
			if _, ok := m.metaRangeMap[metaRangeId]; !ok {
				metaRangeDataDir := path.Join(m.dataDir, fileInfo.Name())
				metaRangeCfg := MetaRangeConfig{
					RangeId:     metaRangeId,
					RootDataDir: metaRangeDataDir,
					Raft:        m.raftServer,
				}
				metaRange := NewMetaRange(metaRangeCfg)
				m.metaRangeMap[metaRangeId] = metaRange
				// Start a goroutine to restore this meta range.
				go metaRange.Restore()
			}
			m.mu.Unlock()
		}
	}
}

func NewMetaRangeManager(dataDir string, raft *raft.RaftServer) *MetaRangeManager {
	return &MetaRangeManager{dataDir: dataDir, raftServer: raft}
}
