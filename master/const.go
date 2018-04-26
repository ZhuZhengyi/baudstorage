package master

const (
	ParaNodeAddr = "addr"
	ParaZoneName = "zone"
	ParaName     = "name"
	ParaId       = "id"
	ParaCount    = "count"
	ParaVolGroup = "vg"
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
	HandleVolOfflineErr = "HandleVolOffLineErr "
)

const (
	DefaultVolSize         = 1024 * 1024 * 1024 * 120
	DefaultMetaTabletRange = 1 << 23
)

//OpCode
const (
	OpCreateVol     = 0x01
	OpDeleteVol     = 0x02
	OpReplicateFile = 0x03
	OpDeleteFile    = 0x04
	OpLoadVol       = 0x05

	OpCreateMetaGroup = 0x06
)

const (
	ClusterGroupID = 1
)

const (
	OptSetNamespace = 1
)

const (
	PrefixNameSpace = "ns"
	KeySeparator    = "#"
)

const (
	OK     = iota
	Failed
)
