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

	dataPartionInfos := make([]*proto.LoadDataPartionResponse, 0)
	space.dataPartionLock.RLock()
	for _, dp := range space.partions {
		dataPartionInfos = append(dataPartionInfos, dp.Load())
	}
	space.dataPartionLock.RUnlock()
	type DisksInfo struct {
		VolInfo []*proto.LoadDataPartionResponse
		Disks   []*Disk
		Rack    string
	}
	diskReport := &DisksInfo{
		VolInfo: dataPartionInfos,
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
	space.dataPartionLock.RLock()
	dataPartionInfos := make([]*proto.LoadDataPartionResponse, 0)
	for _, v := range space.partions {
		dataPartionInfos = append(dataPartionInfos, dp.LoadVol())
	}
	space.dataPartionLock.RUnlock()
	body, _ := json.Marshal(dataPartionInfos)
	w.Write(body)
}
