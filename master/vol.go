package master

import "time"

type Vol struct {
	addr              string
	dataNode          *DataNode
	ReportTime        int64
	FileCount         uint32
	loc               uint8
	status            uint8
	LoadVolIsResponse bool
}

func (v *Vol) CheckVolMiss(volMissSec int64) (isMiss bool) {
	if time.Now().Unix()-v.ReportTime > volMissSec {
		isMiss = true
	}
	return
}

func (v *Vol) IsLive(volTimeOutSec int64) (avail bool) {
	if v.dataNode.isActive == true && v.status != VolUnavailable &&
		v.IsActive(volTimeOutSec) == true {
		avail = true
	}

	return
}

func (v *Vol) IsActive(volTimeOutSec int64) bool {
	return time.Now().Unix()-v.ReportTime <= volTimeOutSec
}

func (v *Vol) GetVolLocationNode() (node *DataNode) {
	return v.dataNode
}
