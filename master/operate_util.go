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
	case OpCreateMetaGroup:
		response = &proto.CreateMetaRangeResponse{}
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
