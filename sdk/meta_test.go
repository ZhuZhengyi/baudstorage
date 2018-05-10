package sdk

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"math/rand"
	"net/http"
	"strings"
	"testing"

	"github.com/google/uuid"
)

const (
	TestNamespace  = "metatest"
	TestMasterAddr = "localhost"
	TestHttpPort   = "9900"
)

type testcase struct {
	inode  uint64
	result string
}

var globalNV *NamespaceView

var globalMP = []MetaPartition{
	{"mp001", 1, 100, nil},
	{"mp002", 101, 200, nil},
	{"mp003", 210, 300, nil},
	{"mp004", 301, 400, nil},
}

var globalTests = []testcase{
	{1, "mp001"},
	{100, "mp001"},
	{101, "mp002"},
	{200, "mp002"},
	{201, ""},
	{209, ""},
	{210, "mp003"},
	{220, "mp003"},
	{300, "mp003"},
	{301, "mp004"},
	{400, "mp004"},
	{401, ""},
	{500, ""},
}

func init() {
	globalNV = newNamespaceView(TestNamespace, globalMP)

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

func newNamespaceView(name string, partitions []MetaPartition) *NamespaceView {
	nv := &NamespaceView{
		Name: name,
	}
	nv.MetaPartitions = make([]*MetaPartition, 0)

	for _, p := range partitions {
		mp := newMetaPartition(p.GroupID, p.Start, p.End)
		nv.MetaPartitions = append(nv.MetaPartitions, mp)
	}

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
	if strings.Compare(name, globalNV.Name) != 0 {
		http.Error(w, "No such namespace!", http.StatusBadRequest)
		return
	}

	data, err := json.Marshal(globalNV)
	if err != nil {
		http.Error(w, "JSON marshal failed!", http.StatusInternalServerError)
		return
	}
	w.Write(data)
}

func TestGetNamespaceView(t *testing.T) {
	resp, err := http.Get("http://" + TestMasterAddr + ":" + TestHttpPort + MetaPartitionViewURL + TestNamespace)
	if err != nil {
		t.Fatal(err)
	}

	defer resp.Body.Close()
	data, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}

	nv := &NamespaceView{}
	err = json.Unmarshal(data, nv)
	if err != nil {
		t.Fatal(err)
	}

	for _, mp := range nv.MetaPartitions {
		t.Logf("%v", *mp)
	}
}

func TestMetaPartitionCreate(t *testing.T) {
	mw, err := NewMetaWrapper(TestNamespace, TestMasterAddr+":"+TestHttpPort)
	if err != nil {
		t.Fatal(err)
	}

	mw.RLock()
	defer mw.RUnlock()
	for _, mp := range mw.partitions {
		t.Logf("%v", *mp)
	}

	for _, tc := range globalTests {
		mp := mw.getMetaPartitionByInode(tc.inode)
		if checkResult(mp, tc.result) != 0 {
			t.Fatal(mp)
		}
		t.Logf("PASS: Finding inode = %v , %v", tc.inode, mp)
	}
}

func checkResult(mp *MetaPartition, result string) int {
	var toCompare string
	if mp == nil {
		toCompare = ""
	} else {
		toCompare = mp.GroupID
	}
	return strings.Compare(toCompare, result)
}
