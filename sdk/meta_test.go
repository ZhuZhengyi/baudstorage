package sdk

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"math/rand"
	"net/http"
	"testing"

	"github.com/google/uuid"
)

const (
	TestNamespace  = "metatest"
	TestMasterAddr = "http://localhost"
	TestHttpPort   = "9900"
)

var (
	globalNV = make(map[string]*NamespaceView)
)

func init() {
	nv := newNamespaceView(TestNamespace, 5)
	globalNV[TestNamespace] = nv

	go func() {
		http.HandleFunc("/client/namespace", handleClientNS)
		err := http.ListenAndServe(":"+TestHttpPort, nil)
		if err != nil {
			fmt.Println(err)
		} else {
			fmt.Println("Done!")
		}
	}()
}

func newNamespaceView(name string, partitions int) *NamespaceView {
	nv := &NamespaceView{
		Name: name,
	}
	nv.MetaPartitions = make([]*MetaPartition, 0)
	nv.generateMetaPartitions(1, partitions)
	return nv
}

func newMetaPartition(gid string, start, end uint64) *MetaPartition {
	return &MetaPartition{
		GroupID: gid,
		Start:   start,
		End:     end,
	}
}

func (nv *NamespaceView) generateMetaPartitions(start, count int) {
	for i := 0; i < count; i++ {
		uuid := uuid.New()
		end := start + rand.Intn(100) + 1
		mp := newMetaPartition(uuid.String(), uint64(start), uint64(end))
		nv.MetaPartitions = append(nv.MetaPartitions, mp)
		start = end + rand.Intn(20) + 1
	}
}

func handleClientNS(w http.ResponseWriter, r *http.Request) {
	r.ParseForm()
	name := r.FormValue("name")
	nv, ok := globalNV[name]
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

func TestGetNamespaceView(t *testing.T) {
	resp, err := http.Get(TestMasterAddr + ":" + TestHttpPort + MetaPartitionViewURL + TestNamespace)
	if err != nil {
		t.Fatal(err)
	}

	data, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()

	nv := &NamespaceView{}
	err = json.Unmarshal(data, nv)
	if err != nil {
		t.Fatal(err)
	}

	for _, mp := range nv.MetaPartitions {
		t.Logf("%v", *mp)
	}
}
