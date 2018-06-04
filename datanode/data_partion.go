package datanode

import (
	"encoding/json"
	"fmt"
	"github.com/juju/errors"
	"github.com/tiglabs/baudstorage/master"
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
	ErrLackOfGoal             = errors.New("dataPartitionGoal is not equal dataPartitionHosts")
	ErrDataPartitionOnBadDisk = errors.New("error bad disk")
)

type DataPartition struct {
	partitionId     uint32
	partitionType   string
	partitionStatus int
	partitionSize   int
	replicaHosts    []string
	isLeader        bool
	path            string
	diskPath        string
	used            int
	store           interface{}
	exitCh          chan bool
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
	if dp.partitionStatus == storage.DiskErrStore {
		return
	}
	dp.isLeader = false
	isLeader, replicas, err := dp.getMembers()
	if !isLeader {
		err = ErrNotLeader
		return
	}
	dp.isLeader = isLeader
	dp.replicaHosts = replicas
	return nil
}

func (dp *DataPartition) getMembers() (isLeader bool, replicaHosts []string, err error) {
	var (
		HostsBuf []byte
	)

	url := fmt.Sprintf(AdminGetDataPartition+"?id=%v", dp.partitionId)
	if HostsBuf, err = PostToMaster(nil, url); err != nil {
		return false, nil, err
	}
	response := &master.DataPartition{}

	if err = json.Unmarshal(HostsBuf, &response); err != nil {
		log.LogError(fmt.Sprintf(ActionGetFollowers+" v[%v] json unmarshal [%v] err[%v]", dp.partitionId, string(HostsBuf), err))
		isLeader = false
		replicaHosts = nil
		return
	}

	if response.Locations != nil && len(response.Locations) >= 1 && response.Locations[0].Addr != LocalIP {
		err = errors.Annotatef(ErrNotLeader, "dataPartition[%v] current LocalIP[%v]", dp.partitionId, LocalIP)
		isLeader = false
		replicaHosts = nil
		return
	}

	if int(response.ReplicaNum) < LeastGoalNum || len(response.Locations) < int(response.ReplicaNum) {
		err = ErrLackOfGoal
		isLeader = false
		replicaHosts = nil
		return
	}

	if response.Status == master.DataPartitionUnavailable || dp.partitionStatus == storage.DiskErrStore {
		err = ErrDataPartitionOnBadDisk
		isLeader = false
		replicaHosts = nil
		return
	}

	for _, location := range response.Locations {
		replicaHosts = append(replicaHosts, location.Addr)
	}
	isLeader = true

	return
}

func (dp *DataPartition) Load() (response *proto.LoadDataPartitionResponse) {
	response = new(proto.LoadDataPartitionResponse)
	response.PartitionId = uint64(dp.partitionId)
	response.PartitionStatus = uint8(dp.partitionStatus)
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
