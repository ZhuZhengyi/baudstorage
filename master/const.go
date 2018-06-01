package master

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
	HandleVolOfflineErr         = "HandleVolOffLineErr "
)

const (
	UnderlineSeparator = "_"
)

const (
	VolUnavailable = 0
	VolReadOnly    = 1
	VolReadWrite   = 2
)

const (
	MetaPartitionUnavailable uint8 = 0
	MetaPartitionReadOnly    uint8 = 1
	MetaPartitionReadWrite   uint8 = 2
)

const (
	DefaultMaxMetaPartitionInodeID  uint64 = 1<<63 - 1
	DefaultMetaPartitionInodeIDStep uint64 = 1 << 32
	DefaultMetaNodeReservedMem      uint64 = 1 << 32
	RuntimeStackBufSize                    = 4096
)

const (
	OK = iota
	Failed
)
