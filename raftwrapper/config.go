package raftwrapper

import (
	"errors"
	"github.com/tiglabs/baudstorage/util/config"
	"github.com/tiglabs/raft/proto"
	"strconv"
	"strings"
)

const (
	OptApplied          = 20
	OptAllocateVolID   = 30
)

const (
	Applied          = "applied"
	MaxVolIDKey     =  "max_vol_id"   //如何定义？？
	TruncateInterval = 20000
	GroupId          = 1
	PartitionNum    = 5
)

const (
	ColonSplit  = ":"
	CommaSplit  = ","
	CfgWalDir   = "walDir"
	CfgStoreDir = "storeDir"
	CfgPeers    = "peers"
)

const (
	HeartbeatPort = 9901
	ReplicatePort = 9902
	GrpcPort      = 9903
)

const (
	RocksDBStorage = "rocksdb"
	//LevelDBStorage = "leveldb"
)

type Config struct {
	peers     []proto.Peer
	peerAddrs []string
	walDir    string
	storeDir  string
}

func NewConfig() *Config {
	return &Config{}
}

func (c *Config) InitConfig(cfg *config.Config) (err error) {
	c.walDir = cfg.GetString(CfgWalDir)
	storeDir := cfg.GetString(CfgStoreDir)
	if storeDir == "" {
		return errors.New("BadConfFile,storeDir is invalid")
	}
	c.storeDir = storeDir
	peerStr := cfg.GetString(CfgPeers)
	if peerStr == "" {
		return errors.New("BadConfFile,peers is invalid")
	}
	if err = c.parsePeers(peerStr); err != nil {
		return errors.New("BadConfFile,peers is invalid")
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

func (c *Config) parsePeers(peerStr string) error {
	peerArr := strings.Split(peerStr, CommaSplit)
	c.peerAddrs = peerArr
	for _, peerAddr := range peerArr {
		id, _, _, err := parsePeerAddr(peerAddr)
		if err != nil {
			return err
		}
		c.peers = append(c.peers, proto.Peer{ID: id})
	}
	return nil
}

func (c *Config) PeerAddrs() []string {
	return c.peerAddrs
}

func (c *Config) Peers() []proto.Peer {
	return c.peers
}

func (c *Config) WalDir() string {
	return c.walDir
}

func (c *Config) StoreDir() string {
	return c.storeDir
}
