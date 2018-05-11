package master

const (
	DefaultReplicaNum                    = 3
	DefaultEveryReleaseVolCount          = 10
	DefaultReleaseVolAfterLoadVolSeconds = 5 * 60
	DefaultReleaseVolInternalSeconds     = 10
	DefaultCheckHeartBeatIntervalSeconds = 60
	DefaultFileDelayCheckLackSec         = 5 * DefaultCheckHeartBeatIntervalSeconds
	DefaultFileDelayCheckCrcSec          = 20 * DefaultCheckHeartBeatIntervalSeconds
	NoHeartBeatTimes                     = 3
	DefaultNodeTimeOutSec                = NoHeartBeatTimes * DefaultCheckHeartBeatIntervalSeconds
	DefaultVolTimeOutSec                 = 5 * DefaultCheckHeartBeatIntervalSeconds
	DefaultVolMissSec                    = 24 * 3600
	DefaultCheckVolIntervalSeconds       = 60
	DefaultVolWarnInterval               = 60 * 60
	LoadVolWaitTime                      = 100
	DefaultLoadVolFrequencyTime          = 60 * 60
	DefaultEveryLoadVolCount             = 10
	DefaultMetaRangeTimeOutSec           = 5 * DefaultCheckHeartBeatIntervalSeconds
)

type ClusterConfig struct {
	FileDelayCheckCrcSec          int64
	FileDelayCheckLackSec         int64
	releaseVolAfterLoadVolSeconds int64
	NodeTimeOutSec                int64
	VolMissSec                    int64
	VolTimeOutSec                 int64
	VolWarnInterval               int64
	LoadVolFrequencyTime          int64
	CheckVolIntervalSeconds       int
	everyReleaseVolCount          int
	everyLoadVolCount             int
	replicaNum                    uint8
}

func NewClusterConfig() (cfg *ClusterConfig) {
	cfg = new(ClusterConfig)
	cfg.replicaNum = DefaultReplicaNum
	cfg.FileDelayCheckCrcSec = DefaultFileDelayCheckCrcSec
	cfg.FileDelayCheckLackSec = DefaultFileDelayCheckLackSec
	cfg.everyReleaseVolCount = DefaultEveryReleaseVolCount
	cfg.releaseVolAfterLoadVolSeconds = DefaultReleaseVolAfterLoadVolSeconds
	cfg.NodeTimeOutSec = DefaultNodeTimeOutSec
	cfg.VolMissSec = DefaultVolMissSec
	cfg.VolTimeOutSec = DefaultVolTimeOutSec
	cfg.CheckVolIntervalSeconds = DefaultCheckVolIntervalSeconds
	cfg.VolWarnInterval = DefaultVolWarnInterval
	cfg.everyLoadVolCount = DefaultEveryLoadVolCount
	cfg.LoadVolFrequencyTime = DefaultLoadVolFrequencyTime
	return
}
