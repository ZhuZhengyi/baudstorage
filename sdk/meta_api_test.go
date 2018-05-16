package sdk

import (
	"fmt"
	"testing"

	"github.com/tiglabs/baudstorage/proto"
)

const (
	SimNamespace  = "simserver"
	SimMasterAddr = "localhost"
	SimMasterPort = "8900"
)

func TestGetNamespace(t *testing.T) {
	mw, err := NewMetaWrapper(SimNamespace, SimMasterAddr+":"+SimMasterPort)
	if err != nil {
		t.Fatal(err)
	}

	for _, mp := range mw.partitions {
		t.Logf("%v", *mp)
	}
}

func TestCreate(t *testing.T) {
	mw, err := NewMetaWrapper(SimNamespace, SimMasterAddr+":"+SimMasterPort)
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 110; i++ {
		name := fmt.Sprintf("abc%v", i)
		info, err := mw.Create_ll(1, name, proto.ModeRegular)
		if err != nil {
			t.Fatal(err)
		}
		t.Logf("inode: %v", *info)
	}
}

func TestInodeGet(t *testing.T) {
	mw, err := NewMetaWrapper(SimNamespace, SimMasterAddr+":"+SimMasterPort)
	if err != nil {
		t.Fatal(err)
	}

	doInodeGet(t, mw, 1)
}

func doInodeGet(t *testing.T, mw *MetaWrapper, ino uint64) {
	info, err := mw.InodeGet_ll(ino)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("Getting inode (%v), %v", ino, *info)
}
