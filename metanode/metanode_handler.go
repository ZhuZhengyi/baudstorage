package metanode

import (
	"encoding/json"
	"net/http"

	"github.com/tiglabs/baudstorage/util/log"
	"strconv"
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
	r.ParseForm()
	id, err := strconv.Atoi(r.FormValue("id"))
	if err != nil {
		w.WriteHeader(400)
		return
	}
	mm := m.metaManager.(*metaManager)
	mm.mu.RLock()
	mp, ok := mm.partitions[uint64(id)]
	mm.mu.RUnlock()
	if !ok {
		w.WriteHeader(404)
		return
	}
	data, _ := json.Marshal(&mp)
	w.Write(data)
}

func (m *MetaNode) updatePeer(w http.ResponseWriter, r *http.Request) {

}
