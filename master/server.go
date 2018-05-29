package master

import (
	"encoding/json"
	"fmt"
	"github.com/tiglabs/baudstorage/raftstore"
	"github.com/tiglabs/baudstorage/util/config"
	"github.com/tiglabs/baudstorage/util/log"
	"strconv"
	"sync"
)

//config keys
const (
	ClusterName = "clusterName"
	ID          = "id"
	IP          = "ip"
	Port        = "port"
	LogDir      = "logDir"
	LogLevel    = "logLevel"
	WalDir      = "walDir"
	StoreDir    = "storeDir"
	GroupId     = 1
	LogModule   = "master"
)

type Master struct {
	id          uint64
	clusterName string
	ip          string
	port        string
	logDir      string
	logLevel    int
	walDir      string
	storeDir    string
	leaderInfo  *LeaderInfo
	config      *ClusterConfig
	cluster     *Cluster
	raftStore   raftstore.RaftStore
	fsm         *MetadataFsm
	partition   raftstore.Partition
	wg          sync.WaitGroup
}

func NewServer() *Master {
	return &Master{}
}

func (m *Master) Start(cfg *config.Config) (err error) {
	m.config = NewClusterConfig()
	m.leaderInfo = &LeaderInfo{}
	if err = m.checkConfig(cfg); err != nil {
		return
	}
	//if _, err = log.NewLog(m.logDir, LogModule, m.logLevel); err != nil {
	//	return
	//}
	if err = m.createRaftServer(); err != nil {
		return
	}
	m.cluster = newCluster(m.clusterName, m.leaderInfo, m.fsm, m.partition)
	m.loadMetadata()
	m.startHttpService()
	m.wg.Add(1)
	return nil
}

func (m *Master) Shutdown() {
	m.wg.Done()
}

func (m *Master) Sync() {
	m.wg.Wait()
}

func (m *Master) checkConfig(cfg *config.Config) (err error) {
	m.clusterName = cfg.GetString(ClusterName)
	m.ip = cfg.GetString(IP)
	m.port = cfg.GetString(Port)
	m.logDir = cfg.GetString(LogDir)
	vfDelayCheckCrcSec := cfg.GetString(FileDelayCheckCrc)
	volMissSec := cfg.GetString(VolMissSec)
	volTimeOutSec := cfg.GetString(VolTimeOutSec)
	everyLoadVolCount := cfg.GetString(EveryLoadVolCount)
	replicaNum := cfg.GetString(ReplicaNum)
	logLevel := cfg.GetString(LogLevel)
	m.walDir = cfg.GetString(WalDir)
	m.storeDir = cfg.GetString(StoreDir)
	peerAddrs := cfg.GetString(CfgPeers)
	if err = m.config.parsePeers(peerAddrs); err != nil {
		return
	}

	if m.id, err = strconv.ParseUint(cfg.GetString(ID), 10, 64); err != nil {
		return fmt.Errorf("%v,err:%v", ErrBadConfFile, err.Error())
	}

	if m.ip == "" || m.port == "" || m.logDir == "" || m.walDir == "" || m.storeDir == "" || m.clusterName == "" {
		return fmt.Errorf("%v,err:%v", ErrBadConfFile, "one of (ip,port,logDir,walDir,storeDir,clusterName) is null")
	}

	if replicaNum != "" {
		if m.config.replicaNum, err = strconv.Atoi(replicaNum); err != nil {
			return fmt.Errorf("%v,err:%v", ErrBadConfFile, err.Error())
		}

		if m.config.replicaNum > 10 {
			return fmt.Errorf("%v,replicaNum(%v) can't too large", ErrBadConfFile, m.config.replicaNum)
		}
	}

	if logLevel != "" {
		if m.logLevel, err = strconv.Atoi(logLevel); err != nil {
			return fmt.Errorf("%v,err:%v", ErrBadConfFile, err.Error())
		}
		if m.logLevel < 0 || m.logLevel > 4 {
			return fmt.Errorf("%v,logLevel(%v) must be between 0 and 4", ErrBadConfFile, m.logLevel)
		}
	}

	if vfDelayCheckCrcSec != "" {
		if m.config.FileDelayCheckCrcSec, err = strconv.ParseInt(vfDelayCheckCrcSec, 10, 0); err != nil {
			return fmt.Errorf("%v,err:%v", ErrBadConfFile, err.Error())
		}
	}

	if volMissSec != "" {
		if m.config.VolMissSec, err = strconv.ParseInt(volMissSec, 10, 0); err != nil {
			return fmt.Errorf("%v,err:%v", ErrBadConfFile, err.Error())
		}
	}
	if volTimeOutSec != "" {
		if m.config.VolTimeOutSec, err = strconv.ParseInt(volTimeOutSec, 10, 0); err != nil {
			return fmt.Errorf("%v,err:%v", ErrBadConfFile, err.Error())
		}
	}
	if everyLoadVolCount != "" {
		if m.config.everyLoadVolCount, err = strconv.Atoi(everyLoadVolCount); err != nil {
			return fmt.Errorf("%v,err:%v", ErrBadConfFile, err.Error())
		}
	}
	if m.config.everyLoadVolCount <= 40 {
		m.config.everyLoadVolCount = 40
	}

	return
}

func (m *Master) createRaftServer() (err error) {
	raftCfg := &raftstore.Config{NodeID: m.id, WalPath: m.walDir}
	if m.raftStore, err = raftstore.NewRaftStore(raftCfg); err != nil {
		return
	}
	fsm := newMetadataFsm(m.storeDir)
	fsm.RegisterLeaderChangeHandler(m.handleLeaderChange)
	fsm.RegisterPeerChangeHandler(m.handlePeerChange)
	fsm.RegisterApplyHandler(m.handleApply)
	fsm.restore()
	m.fsm = fsm
	bytes, _ := json.Marshal(m.config.Peers())
	fmt.Println(string(bytes))
	partitionCfg := &raftstore.PartitionConfig{
		ID:      GroupId,
		Peers:   m.config.Peers(),
		Applied: fsm.applied,
		SM:      fsm,
	}
	if m.partition, err = m.raftStore.CreatePartition(partitionCfg); err != nil {
		return
	}
	return
}
