package master

import (
	"encoding/json"
	"fmt"
	"github.com/tiglabs/baudstorage/proto"
	"github.com/tiglabs/baudstorage/util"
	"github.com/tiglabs/baudstorage/util/log"
)

func newCreateVolRequest(volType string, volId uint64) (req *proto.CreateVolRequest) {
	req = &proto.CreateVolRequest{
		VolType: volType,
		VolId:   volId,
		VolSize: util.DefaultVolSize,
	}
	return
}

func newLoadVolMetricRequest(volType string, volId uint64) (req *proto.LoadVolRequest) {
	req = &proto.LoadVolRequest{
		VolType: volType,
		VolId:   volId,
	}
	return
}

func newDeleteFileRequest(volId uint64, name string) (req *proto.DeleteFileRequest) {
	req = &proto.DeleteFileRequest{
		VolId: volId,
		Name:  name,
	}
	return
}

func UnmarshalTaskResponse(task *proto.AdminTask) (err error) {
	bytes, err := json.Marshal(task.Response)
	if err != nil {
		return
	}
	var response interface{}
	switch task.OpCode {
	case OpCreateVol:
		response = &proto.CreateVolResponse{}
	case OpDeleteVol:
		response = &proto.DeleteVolResponse{}
	case OpLoadVol:
		response = &proto.LoadVolResponse{}
	case OpCreateMetaPartition:
		response = &proto.CreateMetaPartitionResponse{}
	case OpDeleteFile:
		response = &proto.DeleteFileResponse{}
	case OpDeleteMetaPartition:
		response = &proto.DeleteMetaPartitionResponse{}
	case OpUpdateMetaPartition:
		response = &proto.UpdateMetaPartitionResponse{}
	case OpLoadMetaPartition:
		response = task.Response.(*proto.LoadMetaPartitionMetricResponse)
	case OpDataNodeHeartbeat:
		response = &proto.DataNodeHeartBeatResponse{}
	case OpMetaNodeHeartbeat:
		response = &proto.MetaNodeHeartbeatResponse{}
	default:
		log.LogError(fmt.Sprintf("unknown operate code(%v)", task.OpCode))
	}

	if response == nil {
		return fmt.Errorf("UnmarshalTaskResponse failed")
	}
	if err = json.Unmarshal(bytes, response); err != nil {
		return
	}
	task.Response = response
	return
}

func contains(arr []string, element string) (ok bool) {
	if arr == nil || len(arr) == 0 {
		return
	}

	for _, e := range arr {
		if e == element {
			ok = true
			break
		}
	}
	return
}

func remove(arr []string, element string) (newArr []string, ok bool) {
	newArr = make([]string, 0)
	for index, addr := range arr {
		if addr == element {
			after := arr[index+1:]
			newArr = append(newArr, arr[:index]...)
			newArr = append(newArr, after...)
			ok = true
			break
		}
	}

	return
}
