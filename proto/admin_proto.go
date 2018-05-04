package proto

/*
 this struct is used to master send command to metanode
  or send command to datanode
*/

type CreateVolRequest struct {
	VolType string
	VolId   uint64
	VolSize int
}

type CreateVolResponse struct {
	Status uint8
	Result string
}

type LoadVolMetricRequest struct {
	VolType string
	VolId   uint64
}

type LoadVolMetricResponse struct {
	VolType string
	VolId   uint64
	Used    uint64
	Status  uint8
	Result  string
}

type LoadMetaRangeMetricRequest struct {
	Start uint64
	End   uint64
}

type LoadMetaRangeMetricResponse struct {
	Start    uint64
	End      uint64
	MaxInode uint64
	Status   uint8
	Result   string
}

type HeartBeatRequest struct {
	CurrTime int64
}

type HeartBeatResponse struct {
	MaxDiskAvailWeight int64
	Total              uint64 `json:"TotalWeight"`
	Used               uint64 `json:"UsedWeight"`
	ZoneName           string `json:"Zone"`
	Status             uint8
	Result             string
}

type DeleteFileRequest struct {
	VolId uint64
	Name  string
}

type DeleteFileResponse struct {
	Status uint8
	Result string
}
