package datanode

const (
	CfgDisksInfoSplit    = ";"
	CfgDisksContentSplit = ":"
	VolNameJoin          = "_"
	RepairDataSplit      = "/"
	InOutFlowSplit       = "/"
	LogModule            = "datanode"
)

//ump
const (
	AliveTimeInterval = 5 //second
	DefaultUmpAppName = "ump.baud.storage"
)

const (
	ShutdownChanSize   = 1024
	RequstChanSize     = 10240
	ExitChanSize       = 2
	UnitConnPoolSize   = 30
	DefaultDisksSize   = 10
	DefaultVolsSize    = 1024
	LeastAvailDiskNum  = 1
	DefaultTaskChanLen = 1024
	HttpPostMaxRetry   = 3
)

const ChooseDiskTimes = 3

//event kind
const (
	EventKindShowInnerInfo = 1
	ShowDisks              = "disks"
	ShowVols               = "vols"
	ShowStats              = "stats"
)

const (
	ForceClostConnect = true
	NOClostConnect    = false
)

//shutdown
const (
	NotifyShutdownSleepTime = 100 //ms
	MaxShutdownSleepTime    = 100 //s
	FinalShutdownSleepTime  = 1   //s
)

//the response of master cmd
const (
	CmdFail    = -1
	CmdRunning = 1
	CmdSuccess = 2
)

//about conn
const (
	ReadDeadlineTime   = 5
	NoReadDeadlineTime = -1
)

const (
	ActionSendToNext                                 = "ActionSendToNext"
	LocalProcessAddr                                 = "LocalProcess"
	ActionReciveFromNext                             = "ActionReciveFromNext"
	ActionReciveFromNextFinish                       = "ActionReciveFromNextFinish"
	ActionStreamRead                                 = "ActionStreamRead"
	ActionDeletePkgFromList                          = "ActionDeletePkgFromList"
	ActionLocalOperation                             = "ActionLocalOperation_"
	ActionWriteToCli                                 = "ActionWriteToCli"
	ActionPutPkgToReplyChan                          = "ActionPutPkgToReplyChan"
	ActionReadFromCliAndDeal                         = "ActionReadFromCliAndDeal"
	ActionPutSignalToHandleChan                      = "ActionPutSignalToHandleChan"
	ActioncheckAndAddInfos                           = "ActioncheckAndAddInfos"
	ActionCheckChunkInfo                             = "ActionCheckChunkInfo"
	ActionLeaderSetInfo                              = "ActionLeaderSetInfo"
	ActionsyncDelNeedles                             = "ActionsyncDelNeedles"
	ActionRepairChunks                               = "ActionRepairChunks"
	ActionNotifyRepair                               = "ActionNotifyRepair"
	ActionNotifyCompact                              = "ActionNotifyCompact"
	ActionRepairChunksAndSyncNeedls                  = "ActionRepairChunksAndSyncNeedls"
	ActionGetNeedRepairChunks                        = "ActionGetRepairChunks"
	ActionGetFollowerChunkMeta                       = "ActionGetFollowerChunkMeta"
	ActionCompactVol                                 = "ActionCompactVol"
	ActionCompactChunk                               = "ActionCompactChunk"
	ActionRepairPrepare                              = "ActionRepairPrepare"
	ActionRepairGetVolFollowers                      = "ActionRepairGetVolFollowers"
	ActionLeaderSetInfoGetAvaliChunkId               = "ActionLeaderSetInfoGetAvaliChunkId"
	ActionRecvWaitPkg                                = "ActionRecvWaitPkg"
	ActionsendWaitPkg                                = "ActionsendWaitPkg"
	ActionPostToMaster                               = "ActionPostToMaster"
	ActionsendAndRecvWaitPkg                         = "ActionsendAndRecvWaitPkg"
	ActionFollowerToLeaderOpCRepairReadSendRequest   = "ActionFollowerToLeaderOpCRepairReadSendRequest"
	ActionLeaderToFollowerOpCRepairReadRecvResponse  = "ActionLeaderToFollowerOpCRepairReadRecvResponse"
	ActionLeaderToFollowerOpCRepairReadPackResponse  = "ActionLeaderToFollowerOpCRepairReadPackResponse"
	ActionLeaderToFollowerOpRepairReadPackBuffer     = "ActionLeaderToFollowerOpRepairReadPackBuffer"
	ActionLeaderToFollowerOpRepairReadSendPackBuffer = "ActionLeaderToFollowerOpRepairReadSendPackBuffer"

	ActionDoRepairChunk   = "ActionDoRepairChunk"
	ActionDoRepairExtent  = "ActionDoRepairExtent"
	ActionGetFoolwers     = "ActionGetFoolwers"
	ActionCheckReplyAvail = "ActionCheckReplyAvail"
	ActionRepairBatchRead = "ActionRepairBatchRead"

	ActionExitStreamProcess = "ActionExitStreamProcess"
)

const (
	EmptyMsg = ""
)

//stats
const (
	ReportToMonitorRole        = 1
	ReportToMasterRole         = 2
	ReportToSelfRole           = 3
	ReportToMonitorStats       = "ReportStatsToMonitor"
	ReportToMasterStats        = "ReportStatsToMaster"
	ReportToMonitIntervalTime  = 5   //min
	ReportToMasterIntervalTime = 1   //min
	WarningTime                = 600 //ms
	MB                         = 1 << 20
	GB                         = 1 << 30
	InFlow                     = true
	OutFlow                    = false
)

//event
const (
	//get master info
	InitTryGetMasterInfoTimes = 3
	GetMasterInfoIntervalTime = 1 //s

	//the cache of fds
	MaxFds               = (1 << 13)
	CheckFdsIntervalTime = 10

	//modify vols status
	ModifyVolsStatusIntervalTime = 2 //min

	MinWritableChunkCnt = 1
)

//buf unit pool
const (
	PkgDelAndCrcSize            = 4
	PkgArgPoolMaxCap            = 10240
	PkgExtentDataMaxSize        = 1 << 16
	PkgChunkDataMaxSize         = 1<<20 + PkgDelAndCrcSize
	PkgRepairChunkDataMaxSize   = 1<<20 + PkgDelAndCrcSize + NeedleIndexSize
	PkgRepairDataMaxSize        = 1 << 16
	PkgDataPoolMaxCap           = 204800
	PkgHeaderPoolMaxCap         = 204800
	PkgRepairCReadRespLimitSize = 10 * 1024 * 1024
	PkgRepairCReadRespMaxSize   = 15 * 1024 * 1024
)

//write to conn if Free body space
const (
	FreeBodySpace    = true
	NotFreeBodySpace = false
)

const (
	NetType = "tcp"
)

const (
	ObjectIDSize = 8
)

//pack cmd response
const (
	NoFlag    = 0
	ReadFlag  = 1
	WriteFlag = 2
)

const (
	HandleChunk   = "HandleChunk"
	HandleReqs    = "HandleReqs"
	ConnIsNullErr = "ConnIsNullErr"
	GroupNetErr   = "GroupNetErr"
	CreateVolErr  = "CreateVolErr"
)

const (
	LogHeartbeat     = "HB:"
	LogStats         = "Stats:"
	LogLoad          = "Load:"
	LogExit          = "Exit:"
	LogShutdown      = "Shutdown:"
	LogCreateVol     = "CRV:"
	LogCreateFile    = "CRF:"
	LogDelVol        = "DELV:"
	LogDelFile       = "DELF:"
	LogMarkDel       = "MDEL:"
	LogVolSnapshot   = "Snapshot:"
	LogGetWm         = "WM:"
	LogGetAllWm      = "AllWM:"
	LogCompactChunk  = "CompactChunk:"
	LogWrite         = "WR:"
	LogRead          = "RD:"
	LogRepairRead    = "RRD:"
	LogStreamRead    = "SRD:"
	LogRepairNeedles = "RN:"
	LogRepair        = "Repair:"
	LogChecker       = "Checker:"
	LogTask          = "Master Task:"
	LogGetFlow       = "GetFlowInfo:"
)
