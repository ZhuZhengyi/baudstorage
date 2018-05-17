package meta

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

var extraMP = []MetaPartition{
	{"mp004", 320, 390, nil},
	{"mp006", 600, 700, nil},
}

var extraTests = []testcase{
	{301, ""},
	{319, ""},
	{320, "mp004"},
	{390, "mp004"},
	{391, ""},
	{400, ""},
	{599, ""},
	{600, "mp006"},
	{700, "mp006"},
	{701, ""},
}

var getNextTests = []testcase{
	{0, "mp001"},
	{1, "mp002"},
	{101, "mp003"},
	{301, "mp004"},
	{320, "mp006"},
	{600, ""},
	{700, ""},
}

func init() {
	globalNV = &NamespaceView{
		Name:           TestNamespace,
		MetaPartitions: make([]*MetaPartition, 0),
	}

	globalNV.update(globalMP)

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

func (nv *NamespaceView) update(partitions []MetaPartition) {
	for _, p := range partitions {
		mp := newMetaPartition(p.PartitionID, p.Start, p.End)
		nv.MetaPartitions = append(nv.MetaPartitions, mp)
	}
}

func newMetaPartition(id string, start, end uint64) *MetaPartition {
	return &MetaPartition{
		PartitionID: id,
		Start:       start,
		End:         end,
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
	for _, mp := range mw.partitions {
		t.Logf("%v", *mp)
	}
	mw.RUnlock()
}

func TestMetaPartitionFind(t *testing.T) {
	mw, err := NewMetaWrapper(TestNamespace, TestMasterAddr+":"+TestHttpPort)
	if err != nil {
		t.Fatal(err)
	}
	doTest(t, mw, 0, globalTests)
}

func TestMetaPartitionUpdate(t *testing.T) {
	mw, err := NewMetaWrapper(TestNamespace, TestMasterAddr+":"+TestHttpPort)
	if err != nil {
		t.Fatal(err)
	}

	globalNV.update(extraMP)
	err = mw.Update()
	if err != nil {
		t.Fatal(err)
	}

	mw.RLock()
	for _, mp := range mw.partitions {
		t.Logf("%v", *mp)
	}
	mw.RUnlock()

	doTest(t, mw, 0, extraTests)
}

func TestGetNextMetaPartition(t *testing.T) {
	mw, err := NewMetaWrapper(TestNamespace, TestMasterAddr+":"+TestHttpPort)
	if err != nil {
		t.Fatal(err)
	}

	doTest(t, mw, 1, getNextTests)
}

func doTest(t *testing.T, mw *MetaWrapper, op int, tests []testcase) {
	var mp *MetaPartition
	for _, tc := range tests {
		switch op {
		case 1:
			mp = mw.getNextPartition(tc.inode)
		default:
			mp = mw.getPartitionByInode(tc.inode)
		}
		if checkResult(mp, tc.result) != 0 {
			t.Fatalf("inode = %v, %v", tc.inode, mp)
		}
		t.Logf("PASS: Finding inode = %v , %v", tc.inode, mp)
	}
}

func checkResult(mp *MetaPartition, result string) int {
	var toCompare string
	if mp == nil {
		toCompare = ""
	} else {
		toCompare = mp.PartitionID
	}
	return strings.Compare(toCompare, result)
}
