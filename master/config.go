package master

import (
	"fmt"
	"github.com/tiglabs/baudstorage/raftstore"
	"github.com/tiglabs/raft/proto"
	"strconv"
	"strings"
)

const (
	ColonSplit        = ":"
	CommaSplit        = ","
	CfgPeers          = "peers"
	VolMissSec        = "volMissSec"
	VolTimeOutSec     = "volTimeOutSec"
	EveryLoadVolCount = "everyLoadVolCount"
	FileDelayCheckCrc = "fileDelayCheckCrc"
	ReplicaNum        = "replicaNum"
)

const (
	DefaultEveryReleaseVolCount                  = 10
	DefaultReleaseVolAfterLoadVolSeconds         = 5 * 60
	DefaultReleaseVolInternalSeconds             = 10
	DefaultCheckHeartbeatIntervalSeconds         = 60
	DefaultCheckVolIntervalSeconds               = 60
	DefaultFileDelayCheckLackSec                 = 5 * DefaultCheckHeartbeatIntervalSeconds
	DefaultFileDelayCheckCrcSec                  = 20 * DefaultCheckHeartbeatIntervalSeconds
	NoHeartBeatTimes                             = 3
	DefaultNodeTimeOutSec                        = NoHeartBeatTimes * DefaultCheckHeartbeatIntervalSeconds
	DefaultVolTimeOutSec                         = 5 * DefaultCheckHeartbeatIntervalSeconds
	DefaultVolMissSec                            = 24 * 3600
	DefaultVolWarnInterval                       = 60 * 60
	LoadVolWaitTime                              = 100
	DefaultLoadVolFrequencyTime                  = 60 * 60
	DefaultEveryLoadVolCount                     = 10
	DefaultMetaPartitionTimeOutSec               = 5 * DefaultCheckHeartbeatIntervalSeconds
	DefaultMetaPartitionMissSec                  = 3600
	DefaultMetaPartitionWarnInterval             = 10 * 60
	DefaultMetaPartitionThreshold        float32 = 0.75
)

//AddrDatabase ...
var AddrDatabase = make(map[uint64]string)

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
	replicaNum                    int

	peers     []raftstore.PeerAddress
	peerAddrs []string
}

func NewClusterConfig() (cfg *ClusterConfig) {
	cfg = new(ClusterConfig)
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

func parsePeerAddr(peerAddr string) (id uint64, ip string, port uint64, err error) {
	peerStr := strings.Split(peerAddr, ColonSplit)
	id, err = strconv.ParseUint(peerStr[0], 10, 64)
	if err != nil {
		return
	}
	port, err = strconv.ParseUint(peerStr[2], 10, 64)
	if err != nil {
		return
	}
	ip = peerStr[1]
	return
}

func (cfg *ClusterConfig) parsePeers(peerStr string) error {
	peerArr := strings.Split(peerStr, CommaSplit)
	cfg.peerAddrs = peerArr
	for _, peerAddr := range peerArr {
		id, ip, port, err := parsePeerAddr(peerAddr)
		if err != nil {
			return err
		}
		cfg.peers = append(cfg.peers, raftstore.PeerAddress{Peer: proto.Peer{ID: id}, Address: ip})
		address := fmt.Sprintf("%v:%v", ip, port)
		fmt.Println(address)
		AddrDatabase[id] = address
	}
	return nil
}
