package datanode

import (
	"fmt"
	"github.com/tiglabs/baudstorage/storage"
	"os"
	"path"
)

const (
	VolPrefix    = "vol_"
	ExtentVol    = "extent"
	TinyVol      = "tiny"
	EmptyVolName = ""
)

type Vol struct {
	volId    uint32
	volMode  string
	path     string
	diskPath string
	volSize  int
	store    interface{}
	status   int
	isLeader bool

	server *DataNode
}

func NewVol(volId uint32, volMode, name, diskPath string, storeMode bool, storeSize int) (v *Vol, err error) {
	v = new(Vol)
	v.volId = volId
	v.volMode = volMode
	v.diskPath = diskPath
	v.path = name
	if name == EmptyVolName {
		v.path = path.Join(v.diskPath, v.toName())
	}
	switch volMode {
	case ExtentVol:
		v.store, err = storage.NewExtentStore(v.path, storeSize, storeMode)
	case TinyVol:
		v.store, err = storage.NewTinyStore(v.path, storeSize, storeMode)
	default:
		return nil, fmt.Errorf("NewVol[%v] WrongVolMode[%v]", volId, volMode)
	}

	return
}

func (v *Vol) toName() (m string) {
	return fmt.Sprintf(VolPrefix+v.volMode+"_%v_%v", v.volId, v.volSize)
}
