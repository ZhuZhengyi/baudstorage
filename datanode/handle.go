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

	volInfos := make([]*proto.LoadDataPartionResponse, 0)
	space.volLock.RLock()
	for _, v := range space.vols {
		volInfos = append(volInfos, dp.LoadVol())
	}
	space.volLock.RUnlock()
	type DisksInfo struct {
		VolInfo []*proto.LoadDataPartionResponse
		Disks   []*Disk
		Rack    string
	}
	diskReport := &DisksInfo{
		VolInfo: volInfos,
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
	space.volLock.RLock()
	volInfos := make([]*proto.LoadDataPartionResponse, 0)
	for _, v := range space.vols {
		volInfos = append(volInfos, dp.LoadVol())
	}
	space.volLock.RUnlock()
	body, _ := json.Marshal(volInfos)
	w.Write(body)
}
