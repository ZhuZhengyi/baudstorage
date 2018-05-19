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

	volInfos := make([]*proto.VolReport, 0)
	space.volLock.RLock()
	for _, v := range space.vols {
		vr := &proto.VolReport{VolID: uint64(v.volId), VolStatus: v.status, Total: uint64(v.volSize), Used: uint64(v.used)}
		volInfos = append(volInfos, vr)
	}
	space.volLock.RUnlock()
	type DisksInfo struct {
		VolInfo []*proto.VolReport
		Disks   []*Disk
		Rack    string
	}
	diskReport := &DisksInfo{
		VolInfo: volInfos,
		Disks:   disks,
		Rack:    s.zone,
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
