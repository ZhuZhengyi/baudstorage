package master

import "time"

type Vol struct {
	Addr              string
	dataNode          *DataNode
	ReportTime        int64
	FileCount         uint32
	loc               uint8
	Status            uint8
	LoadVolIsResponse bool
	Total             uint64 `json:"TotalSize"`
	Used              uint64 `json:"UsedSize"`
}

func NewVol(dataNode *DataNode) (v *Vol) {
	v = new(Vol)
	v.dataNode = dataNode
	v.Addr = dataNode.HttpAddr
	v.ReportTime = time.Now().Unix()
	return
}

func (v *Vol) SetVolAlive() {
	v.ReportTime = time.Now().Unix()
}

func (v *Vol) CheckVolMiss(volMissSec int64) (isMiss bool) {
	if time.Now().Unix()-v.ReportTime > volMissSec {
		isMiss = true
	}
	return
}

func (v *Vol) IsLive(volTimeOutSec int64) (avail bool) {
	if v.dataNode.isActive == true && v.Status != VolUnavailable &&
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

/*check vol location is avail ,must IsActive=true and volLoc.Status!=volUnavailable*/
func (v *Vol) CheckLocIsAvailContainsDiskError() (avail bool) {
	dataNode := v.GetVolLocationNode()
	dataNode.Lock()
	defer dataNode.Unlock()
	if dataNode.isActive == true && v.IsActive(DefaultVolTimeOutSec) == true {
		avail = true
	}

	return
}
