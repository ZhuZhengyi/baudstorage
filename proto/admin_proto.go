package proto

/*
 this struct is used to master send command to metanode
  or send command to datanode
*/

type RegisterMetaNodeResp struct {
	ID uint64
}

type ClusterInfo struct {
	Cluster string
	Ip      string
}

type CreateVolRequest struct {
	VolType string
	VolId   uint64
	VolSize int
}

type CreateVolResponse struct {
	VolId  uint64
	Status uint8
	Result string
}

type DeleteVolRequest struct {
	VolType string
	VolId   uint64
	VolSize int
}

type DeleteVolResponse struct {
	Status uint8
	Result string
	VolId  uint64
}

type LoadVolRequest struct {
	VolType string
	VolId   uint64
}

type LoadVolResponse struct {
	VolType     string
	VolId       uint64
	Used        uint64
	VolSnapshot []*File
	Status      uint8
	Result      string
}

type File struct {
	Name      string
	Crc       uint32
	CheckSum  uint32
	Size      uint32
	Modified  int64
	MarkDel   bool
	LastObjID uint64
	NeedleCnt int
}

type LoadMetaPartitionMetricRequest struct {
	PartitionID uint64
	Start       uint64
	End         uint64
}

type LoadMetaPartitionMetricResponse struct {
	Start    uint64
	End      uint64
	MaxInode uint64
	Status   uint8
	Result   string
}

type HeartBeatRequest struct {
	CurrTime   int64
	MasterAddr string
}

type VolReport struct {
	VolID     uint64
	VolStatus int
	Total     uint64
	Used      uint64
}

type DataNodeHeartBeatResponse struct {
	Total                     uint64
	Used                      uint64
	Free                      uint64
	CreatedVolWeights         uint64 //volCnt*volsize
	RemainWeightsForCreateVol uint64 //all-usedvolsWieghts
	CreatedVolCnt             uint32
	MaxWeightsForCreateVol    uint64
	RackName                  string
	VolInfo                   []*VolReport
	Status                    uint8
	Result                    string
}

type MetaPartitionReport struct {
	PartitionID uint64
	Start uint64
	End uint64
	Status      int
	MaxInodeID  uint64
	IsLeader    bool
}

type MetaNodeHeartbeatResponse struct {
	RackName                  string
	Total             uint64
	Used              uint64
	MetaPartitionInfo []*MetaPartitionReport
	Status            uint8
	Result            string
}

type DeleteFileRequest struct {
	VolId uint64
	Name  string
}

type DeleteFileResponse struct {
	Status uint8
	Result string
	VolId  uint64
	Name   string
}

type DeleteMetaPartitionRequest struct {
	PartitionID uint64
}

type DeleteMetaPartitionResponse struct {
	PartitionID uint64
	Status      uint8
	Result      string
}

type UpdateMetaPartitionRequest struct {
	PartitionID uint64
	NsName      string
	Start       uint64
	End         uint64
}

type UpdateMetaPartitionResponse struct {
	PartitionID uint64
	NsName      string
	End         uint64
	Status      uint8
	Result      string
}

type MetaPartitionOfflineRequest struct {
	PartitionID uint64
	NsName      string
	RemovePeer  Peer
	AddPeer     Peer
}

type MetaPartitionOfflineResponse struct {
	PartitionID uint64
	NsName      string
	Status      uint8
	Result      string
}
