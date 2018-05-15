package sdk

import (
	"testing"
	//	"github.com/tiglabs/baudstorage/proto"
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
		info, err := mw.Create_ll(1, "abc", 0)
		if err != nil {
			t.Fatal(err)
		}
		t.Logf("inode: %v", *info)
	}

}
