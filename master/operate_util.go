package master

import (
	"github.com/tiglabs/baudstorage/proto"
	"github.com/tiglabs/baudstorage/util"
)

func newCreateVolRequest(volType string, volId uint64) (req *proto.CreateVolRequest) {
	req = &proto.CreateVolRequest{
		VolType: volType,
		VolId:   volId,
		VolSize: util.DefaultVolSize,
	}
	return
}

func newLoadVolMetricRequest(volType string, volId uint64) (req *proto.LoadVolMetricRequest) {
	req = &proto.LoadVolMetricRequest{
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
