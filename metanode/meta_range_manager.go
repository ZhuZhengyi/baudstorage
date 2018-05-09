package metanode

import (
	"errors"
	"io/ioutil"
	"os"
	"path"
	"strings"
	"sync"
)

const metaManagePrefix = "metaManager_"

// MetaRangeGroup manage all MetaRange and make mapping between namespace and MetaRange.
type MetaRangeManager struct {
	metaRangeMap map[string]*MetaRange // Key: metaRangeId, Val: metaRange
	mu           sync.RWMutex
}

// StoreMeteRange try make mapping between meta range ID and MetaRange.
func (m *MetaRangeManager) SetMetaRange(mr *MetaRange) (err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.metaRangeMap[mr.ID]; ok {
		err = errors.New("metaRange '" + mr.ID + "' is existed!")
		return
	}
	m.metaRangeMap[mr.ID] = mr
	return
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

func (m *MetaRangeManager) Range(f func(id string, mr *MetaRange) bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	for id, m := range m.metaRangeMap {
		if !f(id, m) {
			return
		}
	}
}

// Restore load meta manager snapshot from data file and restore all  meta range
// into this meta range manager.
func (m *MetaRangeManager) RestoreMetaManagers(metaDir string) (err error) {
	// Check metaDir directory
	fileInfo, err := os.Stat(metaDir)
	if err != nil {
		os.MkdirAll(metaDir, 0655)
		err = nil
		return
	}
	if !fileInfo.IsDir() {
		err = errors.New("metaDir must be directory!")
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
				/* Create MetaRange and add metaRangeManager */
				// Create MetaRange
				mr := NewMetaRange(MetaRangeConfig{
					ID:        metaID,
					rootDir:   path.Join(metaDir, fileInfo.Name()),
					isRestore: true,
				})
				if err = mr.RestoreMeta(); err != nil {
					// TODO: log
					return
				}
				// Restore inode btree from inode file
				if err = mr.RestoreInode(); err != nil {
					//TODO: log
					return
				}
				// Restore dentry btree from dentry
				if err = mr.RestoreDentry(); err != nil {
					// TODO: log
					return
				}
				// Add MetaRangeManager
				m.SetMetaRange(mr)
				wg.Done()
			}(metaRangeId, fileInfo)
		}
	}
	wg.Wait()
	return
}

func (m *MetaRangeManager) DeleteMetaRange(id string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.metaRangeMap, id)
}

func NewMetaRangeManager() *MetaRangeManager {
	return &MetaRangeManager{
		metaRangeMap: make(map[string]*MetaRange, 0),
	}
}
