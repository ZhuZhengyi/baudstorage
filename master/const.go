package master

const (
	ParaNodeAddr = "addr"
	ParaName     = "name"
	ParaId       = "id"
	ParaCount    = "count"
	ParaReplicas = "replicas"
	ParaVolGroup = "vg"
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
	VolUnavailable = -1
	VolReadOnly    = 1
	VolReadWrite   = 2
)

const (
	//high 4 bit represent range is available,low 4 bit represent range is writable
	MetaRangeUnavailable uint8 = 0x00
	MetaRangeReadOnly    uint8 = 0x10
	MetaRangeReadWrite   uint8 = 0x11
)

const (
	HandleVolOfflineErr = "HandleVolOffLineErr "
)

const (
	DefaultMaxMetaTabletRange = 1<<64 - 1
	DefaultMinMetaRangeSize   = 1 << 34
	RuntimeStackBufSize       = 4096
)

//OpCode
const (
	OpCreateVol         = 0x01
	OpDeleteVol         = 0x02
	OpReplicateFile     = 0x03
	OpDeleteFile        = 0x04
	OpLoadVol           = 0x05
	OpCreateMetaGroup   = 0x06
	OpDataNodeHeartbeat = 0x07
	OpMetaNodeHeartbeat = 0x08
)

const (
	PrefixNameSpace = "ns"
	KeySeparator    = "#"
)

const (
	OK = iota
	Failed
)
