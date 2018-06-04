package master

import "time"

type DataReplica struct {
	Addr                    string
	dataNode                *DataNode
	ReportTime              int64
	FileCount               uint32
	loc                     uint8
	Status                  uint8
	LoadPartitionIsResponse bool
	Total                   uint64 `json:"TotalSize"`
	Used                    uint64 `json:"UsedSize"`
}

func NewDataReplica(dataNode *DataNode) (replica *DataReplica) {
	replica = new(DataReplica)
	replica.dataNode = dataNode
	replica.Addr = dataNode.Addr
	replica.ReportTime = time.Now().Unix()
	return
}

func (replica *DataReplica) SetAlive() {
	replica.ReportTime = time.Now().Unix()
}

func (replica *DataReplica) CheckMiss(missSec int64) (isMiss bool) {
	if time.Now().Unix()-replica.ReportTime > missSec {
		isMiss = true
	}
	return
}

func (replica *DataReplica) IsLive(timeOutSec int64) (avail bool) {
	if replica.dataNode.isActive == true && replica.Status != VolUnavailable &&
		replica.IsActive(timeOutSec) == true {
		avail = true
	}

	return
}

func (replica *DataReplica) IsActive(timeOutSec int64) bool {
	return time.Now().Unix()-replica.ReportTime <= timeOutSec
}

func (replica *DataReplica) GetVolLocationNode() (node *DataNode) {
	return replica.dataNode
}

/*check replica location is avail ,must IsActive=true and replica.Status!=DataReplicaUnavailable*/
func (replica *DataReplica) CheckLocIsAvailContainsDiskError() (avail bool) {
	dataNode := replica.GetVolLocationNode()
	dataNode.Lock()
	defer dataNode.Unlock()
	if dataNode.isActive == true && replica.IsActive(DefaultVolTimeOutSec) == true {
		avail = true
	}

	return
}
