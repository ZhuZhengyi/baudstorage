package master

import "time"

const (
	ParaNodeAddr = "addr"
	ParaName     = "name"
	ParaId       = "id"
	ParaCount    = "count"
	ParaReplicas = "replicas"
	ParaVolGroup = "vg"
	ParaVolType  = "type"
)

const (
	ExtentVol = "extent"
	ChunkVol  = "chunk"
)

const (
	DeleteExcessReplicationErr  = "DeleteExcessReplicationErr "
	AddLackReplicationErr       = "AddLackReplicationErr "
	CheckVolDiskErrorErr        = "CheckVolDiskErrorErr  "
	GetAvailDataNodeHostsErr    = "GetAvailDataNodeHostsErr "
	GetAvailMetaNodeHostsErr    = "GetAvailMetaNodeHostsErr "
	GetLackFileNodeTaskErr      = "GetLackFileNodeTaskErr "
	DeleteFileInCoreInfo        = "DeleteFileInCoreInfo "
	GetVolLocationFileCountInfo = "GetVolLocationFileCountInfo "
	DataNodeOfflineInfo         = "dataNodeOfflineInfo"
)

const (
	UnderlineSeparator = "_"
)

const (
	NoNeedUpdateVolResponse = false
	NeedUpdateVolResponse   = true
)

const (
	VolUnavailable = 0
	VolReadOnly    = 1
	VolReadWrite   = 2
)

const (
	//high 4 bit represent partition is available,low 4 bit represent partition is writable
	MetaPartitionUnavailable uint8 = 0x00
	MetaPartitionReadOnly    uint8 = 0x10
	MetaPartitionReadWrite   uint8 = 0x11
)

const (
	HandleVolOfflineErr = "HandleVolOffLineErr "
)

const (
	DefaultMaxMetaPartitionRange = 1<<63 - 1
	DefaultMinMetaPartitionRange = 1 << 34
	RuntimeStackBufSize          = 4096
	TaskWorkerInterval           = time.Second * time.Duration(5)
)

const (
	OK = iota
	Failed
)
