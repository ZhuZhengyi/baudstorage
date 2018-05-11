package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"runtime"
	"sync"

	. "github.com/tiglabs/baudstorage/sdk"
)

const (
	SimNamespace  = "simserver"
	SimMasterPort = "9900"
	SimMetaAddr   = "localhost"
	SimMetaPort   = "9910"
)

var globalMP = []MetaPartition{
	{"mp001", 1, 100, nil},
	{"mp002", 101, 200, nil},
	{"mp003", 210, 300, nil},
	{"mp004", 301, 400, nil},
}

type MasterServer struct {
	ns map[string]*NamespaceView
}

func main() {
	fmt.Println("Staring Sim Server ...")
	runtime.GOMAXPROCS(runtime.NumCPU())

	var wg sync.WaitGroup

	ms := NewMasterServer()
	ms.Start(&wg)

	wg.Wait()
}

// Master Server

func NewMasterServer() *MasterServer {
	return &MasterServer{
		ns: make(map[string]*NamespaceView),
	}
}

func (m *MasterServer) Start(wg *sync.WaitGroup) {
	nv := &NamespaceView{
		Name:           SimNamespace,
		MetaPartitions: make([]*MetaPartition, 0),
	}

	for _, p := range globalMP {
		mp := newMetaPartition(p.GroupID, p.Start, p.End, SimMetaAddr+":"+SimMetaPort)
		nv.MetaPartitions = append(nv.MetaPartitions, mp)
	}

	m.ns[nv.Name] = nv

	wg.Add(1)
	go func() {
		defer wg.Done()
		http.HandleFunc("/client/namespace", m.handleClientNS)
		if err := http.ListenAndServe(":"+SimMasterPort, nil); err != nil {
			fmt.Println(err)
		} else {
			fmt.Println("Done!")
		}
	}()
}

func (m *MasterServer) handleClientNS(w http.ResponseWriter, r *http.Request) {
	r.ParseForm()
	name := r.FormValue("name")
	nv, ok := m.ns[name]
	if !ok {
		http.Error(w, "No such namespace!", http.StatusBadRequest)
		return
	}

	data, err := json.Marshal(nv)
	if err != nil {
		http.Error(w, "JSON marshal failed!", http.StatusInternalServerError)
		return
	}
	w.Write(data)
}

func newMetaPartition(gid string, start, end uint64, member string) *MetaPartition {
	return &MetaPartition{
		GroupID: gid,
		Start:   start,
		End:     end,
		Members: []string{member, member, member},
	}
}

// Meta Server
