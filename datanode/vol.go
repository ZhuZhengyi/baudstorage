package datanode

import (
	"encoding/json"
	"fmt"
	"github.com/juju/errors"
	"github.com/tiglabs/baudstorage/proto"
	"github.com/tiglabs/baudstorage/storage"
	"github.com/tiglabs/baudstorage/util/log"
	"path"
)

const (
	VolPrefix    = "vol_"
	EmptyVolName = ""
)

var (
	GetVolMember    = "/datanode/member"
	ErrNotLeader    = errors.New("not leader")
	LeastGoalNum    = 2
	ErrLackOfGoal   = errors.New("volGoal is not equare volhosts")
	ErrVolOnBadDisk = errors.New("error bad disk")
)

type Vol struct {
	volId    uint32
	volMode  string
	path     string
	diskPath string
	volSize  int
	used     int
	store    interface{}
	status   int
	isLeader bool
	members  *VolMembers
	server   *DataNode
	exitCh   chan bool
}

type VolMembers struct {
	VolStatus int32
	VolId     int
	VolGoal   int
	VolHosts  []string
}

func NewVol(volId uint32, volMode, name, diskPath string, storeMode bool, storeSize int) (v *Vol, err error) {
	v = new(Vol)
	v.volId = volId
	v.volMode = volMode
	v.diskPath = diskPath
	v.path = name
	v.volSize = storeSize
	v.exitCh = make(chan bool, 10)
	if name == EmptyVolName {
		v.path = path.Join(v.diskPath, v.toName())
	}
	switch volMode {
	case proto.ExtentVol:
		v.store, err = storage.NewExtentStore(v.path, storeSize, storeMode)
	case proto.TinyVol:
		v.store, err = storage.NewTinyStore(v.path, storeSize, storeMode)
	default:
		return nil, fmt.Errorf("NewDataReplica[%v] WrongVolMode[%v]", volId, volMode)
	}
	go v.checkExtent()
	return
}

func (v *Vol) toName() (m string) {
	return fmt.Sprintf(VolPrefix+v.volMode+"_%v_%v", v.volId, v.volSize)
}

func (v *Vol) parseVolMember() (err error) {
	if v.status == storage.DiskErrStore {
		return
	}
	v.isLeader = false
	isLeader, members, err := v.getMembers()
	if !isLeader {
		err = ErrNotLeader
		return
	}
	v.isLeader = isLeader
	v.members = members
	return nil
}

func (v *Vol) getMembers() (bool, *VolMembers, error) {
	var (
		volHostsBuf []byte
		err         error
	)

	url := fmt.Sprintf(GetVolMember+"?vol=%v", v.volId)
	if volHostsBuf, err = v.server.postToMaster(nil, url); err != nil {
		return false, nil, err
	}
	members := new(VolMembers)

	if err = json.Unmarshal(volHostsBuf, &members); err != nil {
		log.LogError(fmt.Sprintf(ActionGetFoolwers+" v[%v] json unmarshal [%v] err[%v]", v.volId, string(volHostsBuf), err))
		return false, nil, err
	}

	if len(members.VolHosts) >= 1 && members.VolHosts[0] != v.server.localServeAddr {
		err = errors.Annotatef(ErrNotLeader, "vol[%v] current LocalIP[%v]", v.volId, v.server.localServeAddr)
		return false, nil, err
	}

	if members.VolGoal < LeastGoalNum || len(members.VolHosts) < members.VolGoal {
		err = ErrLackOfGoal
		return false, nil, err
	}

	if members.VolStatus == storage.DiskErrStore || v.status == storage.DiskErrStore {
		err = ErrVolOnBadDisk
		return false, nil, err
	}

	return true, members, nil
}

func (v *Vol) LoadVol() (response *proto.LoadVolResponse) {
	response = new(proto.LoadVolResponse)
	response.VolId = uint64(v.volId)
	response.VolStatus = uint8(v.status)
	response.VolType = v.volMode
	response.VolSnapshot = make([]*proto.File, 0)
	switch v.volMode {
	case proto.ExtentVol:
		var err error
		store := v.store.(*storage.ExtentStore)
		response.VolSnapshot, err = store.SnapShot()
		response.Used = uint64(store.GetStoreUsedSize())
		if err != nil {
			response.Status = proto.TaskFail
			response.Result = err.Error()
		} else {
			response.Status = proto.TaskSuccess
		}
	case proto.TinyVol:

	}
	return

}
