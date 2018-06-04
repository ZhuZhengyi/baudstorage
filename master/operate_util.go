package master

import (
	"encoding/json"
	"fmt"
	"github.com/tiglabs/baudstorage/proto"
	"github.com/tiglabs/baudstorage/util"
	"github.com/tiglabs/baudstorage/util/log"
	"github.com/tiglabs/baudstorage/util/ump"
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
	log.LogDebug(fmt.Sprintf("received TaskResponse:%v", string(bytes)))
	var response interface{}
	switch task.OpCode {
	case proto.OpCreateDataPartion:
		response = &proto.CreateVolResponse{}
	case proto.OpDeleteDataPartion:
		response = &proto.DeleteVolResponse{}
	case proto.OpLoadDataPartion:
		response = &proto.LoadVolResponse{}
	case proto.OpCreateMetaPartition:
		response = &proto.CreateMetaPartitionResponse{}
	case proto.OpDeleteFile:
		response = &proto.DeleteFileResponse{}
	case proto.OpDeleteMetaPartition:
		response = &proto.DeleteMetaPartitionResponse{}
	case proto.OpUpdateMetaPartition:
		response = &proto.UpdateMetaPartitionResponse{}
	case proto.OpLoadMetaPartition:
		response = task.Response.(*proto.LoadMetaPartitionMetricResponse)
	case proto.OpDataNodeHeartbeat:
		response = &proto.DataNodeHeartBeatResponse{}
	case proto.OpMetaNodeHeartbeat:
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
	log.LogDebug(fmt.Sprintf("UnmarshalTaskResponse:%v", string(bytes)))
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

func Warn(clusterID, msg string) {
	log.LogWarn(msg)
	umpKey := fmt.Sprintf("%s_%s", clusterID, UmpModuleName)
	ump.Alarm(umpKey, msg)
}
