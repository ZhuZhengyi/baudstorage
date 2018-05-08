package metanode

import (
	"errors"
	"io/ioutil"
	"strings"
	"sync"

	"github.com/tiglabs/baudstorage/util/log"
)

const metaManagePrefix = "metaManager_"

// MetaRangeGroup manage all MetaRange and make mapping between namespace and MetaRange.
type MetaRangeManager struct {
	metaRangeMap map[string]*MetaRange // Key: metaRangeId, Val: metaRange
	mu           sync.RWMutex
}

// StoreMeteRange try make mapping between meta range ID and MetaRange.
func (m *MetaRangeManager) CreateMetaRange(id string, start, end uint64,
	peers []string) (err error) {
	if len(strings.TrimSpace(id)) > 0 {
		m.mu.Lock()
		defer m.mu.Unlock()
		if _, ok := m.metaRangeMap[id]; ok {
			err = errors.New("metaRange '" + id + "' is existed!")
			log.LogError(err.Error())
			return
		}
		metaRange := NewMetaRange(&CreateMetaRangeReq{
			MetaId:  id,
			Start:   start,
			End:     end,
			Members: peers,
		})
		m.metaRangeMap[id] = metaRange
	}
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

// Restore load meta manager snapshot from data file and restore all  meta range
// into this meta range manager.
func (m *MetaRangeManager) RestoreMetaManagers(metaDir string) {
	// Scan data directory.
	fileInfos, err := ioutil.ReadDir(metaDir)
	if err != nil {
		log.LogError("action[MetaRangeManager.Restore],err:%v", err)
		return
	}
	for _, fileInfo := range fileInfos {
		if fileInfo.IsDir() && strings.HasPrefix(fileInfo.Name(),
			metaManagePrefix) {
			metaRangeId := fileInfo.Name()[13:]
			m.mu.Lock()
			if _, ok := m.metaRangeMap[metaRangeId]; !ok {
				m.CreateMetaRange(metaRangeId, 0, 0, []string{})
			}
			m.mu.Unlock()
		}
	}
}

func (m *MetaRangeManager) RestoreRanges() {
	var wg sync.WaitGroup
	for _, mr := range m.metaRangeMap {
		wg.Add(1)
		go func(mr *MetaRange, wg sync.WaitGroup) {
			//read meta
			//restore inode btree
			//restore dentry btree
			wg.Done()
		}(mr, wg)
	}
	wg.Wait()
}

func (m *MetaRangeManager) RestoreApplyIDFromRaft() {

}

func NewMetaRangeManager() *MetaRangeManager {
	return &MetaRangeManager{}
}
