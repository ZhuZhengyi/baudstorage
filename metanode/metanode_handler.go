package metanode

import (
	"encoding/json"
	"github.com/tiglabs/baudstorage/util/log"
	"net/http"
)

func (m *MetaNode) allPartitionsHandle(w http.ResponseWriter, r *http.Request) {
	mm := m.metaManager.(*metaManager)
	mm.mu.RLock()
	defer mm.mu.RUnlock()
	data, err := json.Marshal(mm.partitions)
	if err != nil {
		w.WriteHeader(400)
		log.LogDebugf("[allPartitionsHandle]: %s", err.Error())
		return
	}
	w.Write(data)
}

func (m *MetaNode) inodeInfoHandle(w http.ResponseWriter, r *http.Request) {

}

func (m *MetaNode) updatePeer(w http.ResponseWriter, r *http.Request) {

}
