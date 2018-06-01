package metanode

import (
	"encoding/json"
	"net/http"

	"github.com/tiglabs/baudstorage/util/log"
)

func (m *MetaNode) registerHandler() (err error) {
	// Register http handler
	http.HandleFunc("/getAllPartitions", m.allPartitionsHandle)
	http.HandleFunc("/getInodeInfo", m.inodeInfoHandle)
	return
}
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
