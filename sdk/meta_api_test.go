package sdk

import (
	"testing"
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
