package master

import (
	"fmt"
	"github.com/tiglabs/baudstorage/util"
	"github.com/tiglabs/raft/proto"
	"strconv"
	"strings"
)

const (
	ColonSplit                    = ":"
	CommaSplit                    = ","
	CfgPeers                      = "peers"
	VolMissSec                    = "volMissSec"
	VolTimeOutSec                 = "volTimeOutSec"
	EveryLoadVolCount             = "everyLoadVolCount"
	FileDelayCheckCrc             = "fileDelayCheckCrc"
	ReplicaNum                    = "replicaNum"
	MetaPartitionSize             = "metaPartitionSize"
	VolSize                       = "volSize"
	CheckHeartBeatIntervalSeconds = "heartBeatIntervalSeconds"
)

const (
	DefaultReplicaNum                    = 3
	DefaultEveryReleaseVolCount          = 10
	DefaultReleaseVolAfterLoadVolSeconds = 5 * 60
	DefaultReleaseVolInternalSeconds     = 10
	DefaultCheckHeartbeatIntervalSeconds = 60
	DefaultFileDelayCheckLackSec         = 5 * DefaultCheckHeartbeatIntervalSeconds
	DefaultFileDelayCheckCrcSec          = 20 * DefaultCheckHeartbeatIntervalSeconds
	NoHeartBeatTimes                     = 3
	DefaultNodeTimeOutSec                = NoHeartBeatTimes * DefaultCheckHeartbeatIntervalSeconds
	DefaultVolTimeOutSec                 = 5 * DefaultCheckHeartbeatIntervalSeconds
	DefaultVolMissSec                    = 24 * 3600
	DefaultCheckVolIntervalSeconds       = 60
	DefaultVolWarnInterval               = 60 * 60
	LoadVolWaitTime                      = 100
	DefaultLoadVolFrequencyTime          = 60 * 60
	DefaultEveryLoadVolCount             = 10
	DefaultMetaPartitionTimeOutSec       = 5 * DefaultCheckHeartbeatIntervalSeconds
	DefaultMetaPartitionThreshold        = 0.75
	DefaultMetaPartitionMemSize          = 16 * util.GB
)

//Address ...
type Address struct {
	HttpAddr string
}

//AddrDatabase ...
var AddrDatabase = make(map[uint64]*Address)

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

	peers     []proto.Peer
	peerAddrs []string
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

//AddrInit ...
func AddrInit(peerAddrs []string) (err error) {
	fmt.Println("PeerAddrs:")
	for _, peerAddr := range peerAddrs {
		id, ip, port, err := parsePeerAddr(peerAddr)
		if err != nil {
			return err
		}
		AddrDatabase[id] = &Address{
			HttpAddr: fmt.Sprintf("%s:%d", ip, port),
		}
		fmt.Println(AddrDatabase[id])
	}
	return nil
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
		id, _, _, err := parsePeerAddr(peerAddr)
		if err != nil {
			return err
		}
		cfg.peers = append(cfg.peers, proto.Peer{ID: id})
	}
	return nil
}

func (cfg *ClusterConfig) PeerAddrs() []string {
	return cfg.peerAddrs
}

func (cfg *ClusterConfig) Peers() []proto.Peer {
	return cfg.peers
}
