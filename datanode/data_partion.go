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
	DataPartionPrefix = "datapartion_"
	EmptyVolName      = ""
)

var (
	GetDataPartionMember    = "/datanode/member"
	ErrNotLeader            = errors.New("not leader")
	LeastGoalNum            = 2
	ErrLackOfGoal           = errors.New("dataPartionGoal is not equare dataPartionhosts")
	ErrDataPartionOnBadDisk = errors.New("error bad disk")
)

type DataPartion struct {
	partionId   uint32
	partionType string
	path        string
	diskPath    string
	partionSize int
	used        int
	store       interface{}
	status      int
	isLeader    bool
	members     *DataPartionResponse
	exitCh      chan bool
}

type DataPartionResponse struct {
	PartionId     uint64
	PartionStatus uint8
	ReplicaNum    uint8
	PartionType   string
	Hosts         []string
}

func NewDataPartion(partionId uint32, partionType, name, diskPath string, storeMode bool, storeSize int) (dp *DataPartion, err error) {
	dp = new(DataPartion)
	dp.partionId = partionId
	dp.partionType = partionType
	dp.diskPath = diskPath
	dp.path = name
	dp.partionSize = storeSize
	dp.exitCh = make(chan bool, 10)
	if name == EmptyVolName {
		dp.path = path.Join(dp.diskPath, dp.toName())
	}
	switch partionType {
	case proto.ExtentVol:
		dp.store, err = storage.NewExtentStore(dp.path, storeSize, storeMode)
	case proto.TinyVol:
		dp.store, err = storage.NewTinyStore(dp.path, storeSize, storeMode)
	default:
		return nil, fmt.Errorf("NewDataPartion[%v] WrongVolMode[%v]", partionId, partionType)
	}
	go dp.checkExtent()
	return
}

func (dp *DataPartion) toName() (m string) {
	return fmt.Sprintf(DataPartionPrefix+dp.partionType+"_%v_%v", dp.partionId, dp.partionSize)
}

func (dp *DataPartion) parseVolMember() (err error) {
	if dp.status == storage.DiskErrStore {
		return
	}
	dp.isLeader = false
	isLeader, members, err := dp.getMembers()
	if !isLeader {
		err = ErrNotLeader
		return
	}
	dp.isLeader = isLeader
	dp.members = members
	return nil
}

func (dp *DataPartion) getMembers() (bool, *DataPartionResponse, error) {
	var (
		HostsBuf []byte
		err      error
	)

	url := fmt.Sprintf(GetDataPartionMember+"?dataPartion=%v", dp.partionId)
	if HostsBuf, err = PostToMaster(nil, url); err != nil {
		return false, nil, err
	}
	members := new(DataPartionResponse)

	if err = json.Unmarshal(HostsBuf, &members); err != nil {
		log.LogError(fmt.Sprintf(ActionGetFoolwers+" v[%v] json unmarshal [%v] err[%v]", dp.partionId, string(HostsBuf), err))
		return false, nil, err
	}

	if len(members.Hosts) >= 1 && members.Hosts[0] != LocalIP {
		err = errors.Annotatef(ErrNotLeader, "dataPartion[%v] current LocalIP[%v]", dp.partionId, LocalIP)
		return false, nil, err
	}

	if int(members.ReplicaNum) < LeastGoalNum || len(members.Hosts) < int(members.ReplicaNum) {
		err = ErrLackOfGoal
		return false, nil, err
	}

	if members.PartionStatus == storage.DiskErrStore || dp.status == storage.DiskErrStore {
		err = ErrDataPartionOnBadDisk
		return false, nil, err
	}

	return true, members, nil
}

func (dp *DataPartion) Load() (response *proto.LoadDataPartionResponse) {
	response = new(proto.LoadDataPartionResponse)
	response.PartionId = uint64(dp.partionId)
	response.PartionStatus = uint8(dp.status)
	response.DataPartionType = dp.partionType
	response.PartionSnapshot = make([]*proto.File, 0)
	switch dp.partionType {
	case proto.ExtentVol:
		var err error
		store := dp.store.(*storage.ExtentStore)
		response.PartionSnapshot, err = store.SnapShot()
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
