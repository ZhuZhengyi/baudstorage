package master

const (
	ParaNodeAddr          = "addr"
	ParaName              = "name"
	ParaId                = "id"
	ParaCount             = "count"
	ParaReplicas          = "replicas"
	ParaDataPartitionType = "type"
)

const (
	DeleteExcessReplicationErr     = "DeleteExcessReplicationErr "
	AddLackReplicationErr          = "AddLackReplicationErr "
	CheckDataPartitionDiskErrorErr = "CheckDataPartitionDiskErrorErr  "
	GetAvailDataNodeHostsErr       = "GetAvailDataNodeHostsErr "
	GetAvailMetaNodeHostsErr       = "GetAvailMetaNodeHostsErr "
	GetDataReplicaFileCountInfo    = "GetDataReplicaFileCountInfo "
	DataNodeOfflineInfo            = "dataNodeOfflineInfo"
	HandleDataPartitionOfflineErr  = "HandleDataPartitionOffLineErr "
)

const (
	UnderlineSeparator = "_"
)

const (
	DataPartitionUnavailable = 0
	DataPartitionReadOnly    = 1
	DataPartitionReadWrite   = 2
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
