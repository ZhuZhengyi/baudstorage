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
	DataPartitionPrefix = "datapartition_"
	EmptyVolName        = ""
)

var (
	AdminGetDataPartition     = "/admin/getDataPartition"
	ErrNotLeader              = errors.New("not leader")
	LeastGoalNum              = 2
	ErrLackOfGoal             = errors.New("dataPartitionGoal is not equare dataPartitionhosts")
	ErrDataPartitionOnBadDisk = errors.New("error bad disk")
)

type DataPartition struct {
	partitionId   uint32
	partitionType string
	path          string
	diskPath      string
	partitionSize int
	used          int
	store         interface{}
	status        int
	isLeader      bool
	members       *DataPartitionMembers
	exitCh        chan bool
}

type DataPartitionMembers struct {
	PartitionId     uint64
	PartitionStatus int8
	ReplicaNum      uint8
	PartitionType   string
	Hosts           []string
}

func NewDataPartition(partitionId uint32, partitionType, name, diskPath string, storeMode bool, storeSize int) (dp *DataPartition, err error) {
	dp = new(DataPartition)
	dp.partitionId = partitionId
	dp.partitionType = partitionType
	dp.diskPath = diskPath
	dp.path = name
	dp.partitionSize = storeSize
	dp.exitCh = make(chan bool, 10)
	if name == EmptyVolName {
		dp.path = path.Join(dp.diskPath, dp.toName())
	}
	switch partitionType {
	case proto.ExtentVol:
		dp.store, err = storage.NewExtentStore(dp.path, storeSize, storeMode)
	case proto.TinyVol:
		dp.store, err = storage.NewTinyStore(dp.path, storeSize, storeMode)
	default:
		return nil, fmt.Errorf("NewDataPartition[%v] WrongVolMode[%v]", partitionId, partitionType)
	}
	go dp.checkExtent()
	return
}

func (dp *DataPartition) toName() (m string) {
	return fmt.Sprintf(DataPartitionPrefix+dp.partitionType+"_%v_%v", dp.partitionId, dp.partitionSize)
}

func (dp *DataPartition) parseVolMember() (err error) {
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

func (dp *DataPartition) getMembers() (bool, *DataPartitionMembers, error) {
	var (
		HostsBuf []byte
		err      error
	)

	url := fmt.Sprintf(AdminGetDataPartition+"?id=%v", dp.partitionId)
	if HostsBuf, err = PostToMaster(nil, url); err != nil {
		return false, nil, err
	}
	members := new(DataPartitionMembers)

	if err = json.Unmarshal(HostsBuf, &members); err != nil {
		log.LogError(fmt.Sprintf(ActionGetFoolwers+" v[%v] json unmarshal [%v] err[%v]", dp.partitionId, string(HostsBuf), err))
		return false, nil, err
	}

	if len(members.Hosts) >= 1 && members.Hosts[0] != LocalIP {
		err = errors.Annotatef(ErrNotLeader, "dataPartition[%v] current LocalIP[%v]", dp.partitionId, LocalIP)
		return false, nil, err
	}

	if int(members.ReplicaNum) < LeastGoalNum || len(members.Hosts) < int(members.ReplicaNum) {
		err = ErrLackOfGoal
		return false, nil, err
	}

	if members.PartitionStatus == storage.DiskErrStore || dp.status == storage.DiskErrStore {
		err = ErrDataPartitionOnBadDisk
		return false, nil, err
	}

	return true, members, nil
}

func (dp *DataPartition) Load() (response *proto.LoadDataPartitionResponse) {
	response = new(proto.LoadDataPartitionResponse)
	response.PartitionId = uint64(dp.partitionId)
	response.PartitionStatus = uint8(dp.status)
	response.PartitionType = dp.partitionType
	response.PartitionSnapshot = make([]*proto.File, 0)
	switch dp.partitionType {
	case proto.ExtentVol:
		var err error
		store := dp.store.(*storage.ExtentStore)
		response.PartitionSnapshot, err = store.SnapShot()
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
