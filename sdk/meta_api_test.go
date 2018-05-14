package sdk

import (
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

	mp := mw.getPartitionByID("mp001")
	if mp == nil {
		t.Fatal("No such partition")
	}

	mc, err := mw.getConn(mp)
	if err != nil {
		t.Fatal(err)
	}

	status, info, err := mw.icreate(mc, proto.ModeDir)
	if err != nil {
		t.Fatal(err)
	}

	if status != int(proto.OpOk) {
		t.Fatal("status: ", status)
	}

	t.Logf("ino = %v", info.Inode)
}
