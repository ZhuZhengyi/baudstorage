package metanode

import (
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"strings"
	"sync"
)

const metaManagePrefix = "metaManager_"

// MetaRangeGroup manage all MetaPartition and make mapping between namespace and MetaPartition.
type MetaManager struct {
	dataPath   string
	partitions map[string]*MetaPartition // Key: metaRangeId, Val: metaPartition
	mu         sync.RWMutex
}

// StoreMeteRange try make mapping between meta range ID and MetaPartition.
func (m *MetaManager) SetMetaRange(mr *MetaPartition) (err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.partitions[mr.ID]; ok {
		err = fmt.Errorf("meta partition %d is existed", mr.ID)
		return
	}
	m.partitions[mr.ID] = mr
	return
}

// LoadMetaPartition returns MetaPartition with specified namespace if the mapping exist or report an error.
func (m *MetaManager) LoadMetaPartition(id string) (mr *MetaPartition, err error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	mr, ok := m.partitions[id]
	if ok {
		return
	}
	err = errors.New("unknown meta partition: " + id)
	return
}

func (m *MetaManager) Range(f func(id string, mr *MetaPartition) bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	for id, m := range m.partitions {
		if !f(id, m) {
			return
		}
	}
}

// Load meta manager snapshot from data file and restore all  meta range
// into this meta range manager.
func (m *MetaManager) LoadMetaManagers(metaDir string) (err error) {
	// Check metaDir directory
	fileInfo, err := os.Stat(metaDir)
	if err != nil {
		os.MkdirAll(metaDir, 0655)
		err = nil
		return
	}
	if !fileInfo.IsDir() {
		err = errors.New("metaDir must be directory")
		return
	}
	// Scan data directory.
	fileInfos, err := ioutil.ReadDir(metaDir)
	if err != nil {
		return
	}
	var wg sync.WaitGroup
	for _, fileInfo := range fileInfos {
		if fileInfo.IsDir() && strings.HasPrefix(fileInfo.Name(),
			metaManagePrefix) {
			wg.Add(1)
			metaRangeId := fileInfo.Name()[12:]
			go func(metaID string, fileInfo os.FileInfo) {
				/* Create MetaPartition and add metaManager */
				mr := NewMetaPartition(MetaPartitionConfig{
					ID:      metaID,
					RootDir: path.Join(metaDir, fileInfo.Name()),
				})
				if err = mr.Load(); err != nil {
					// TODO: log
					return
				}
				m.SetMetaRange(mr)
				wg.Done()
			}(metaRangeId, fileInfo)
		}
	}
	wg.Wait()
	return
}

func (m *MetaManager) DeleteMetaRange(id string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.partitions, id)
}

func NewMetaRangeManager() *MetaManager {
	return &MetaManager{
		partitions: make(map[string]*MetaPartition, 0),
	}
}
