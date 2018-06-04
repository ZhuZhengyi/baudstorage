package datanode

import (
	"encoding/json"
	"github.com/tiglabs/baudstorage/proto"
	"net/http"
)

func (s *DataNode) HandleGetDisk(w http.ResponseWriter, r *http.Request) {
	space := s.space
	space.diskLock.RLock()
	disks := make([]*Disk, 0)
	for _, d := range space.disks {
		disks = append(disks, d)
	}
	space.diskLock.RUnlock()

	dataPartitionInfos := make([]*proto.LoadDataPartitionResponse, 0)
	space.dataPartitionLock.RLock()
	for _, dp := range space.partitions {
		dataPartitionInfos = append(dataPartitionInfos, dp.Load())
	}
	space.dataPartitionLock.RUnlock()
	type DisksInfo struct {
		VolInfo []*proto.LoadDataPartitionResponse
		Disks   []*Disk
		Rack    string
	}
	diskReport := &DisksInfo{
		VolInfo: dataPartitionInfos,
		Disks:   disks,
		Rack:    s.rackName,
	}
	body, _ := json.Marshal(diskReport)
	w.Write(body)
}

func (s *DataNode) HandleStat(w http.ResponseWriter, r *http.Request) {
	response := &proto.DataNodeHeartBeatResponse{}
	s.fillHeartBeatResponse(response)
	body, _ := json.Marshal(response)
	w.Write(body)
}

func (s *DataNode) HandleVol(w http.ResponseWriter, r *http.Request) {
	space := s.space
	space.dataPartitionLock.RLock()
	dataPartitionInfos := make([]*proto.LoadDataPartitionResponse, 0)
	for _, dp := range space.partitions {
		dataPartitionInfos = append(dataPartitionInfos, dp.Load())
	}
	space.dataPartitionLock.RUnlock()
	body, _ := json.Marshal(dataPartitionInfos)
	w.Write(body)
}
