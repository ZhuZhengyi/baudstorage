package master

import (
	"fmt"
	"github.com/juju/errors"
	"github.com/tiglabs/baudstorage/raftstore"
	"github.com/tiglabs/baudstorage/util/config"
	"github.com/tiglabs/baudstorage/util/log"
	"github.com/tiglabs/baudstorage/util/ump"
	"strconv"
	"sync"
)

//config keys
const (
	ClusterName   = "clusterName"
	ID            = "id"
	IP            = "ip"
	Port          = "port"
	LogLevel      = "logLevel"
	WalDir        = "walDir"
	StoreDir      = "storeDir"
	GroupId       = 1
	UmpModuleName = "master"
)

type Master struct {
	id          uint64
	clusterName string
	ip          string
	port        string
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
	ump.InitUmp(UmpModuleName)
	if err = m.checkConfig(cfg); err != nil {
		log.LogError(errors.ErrorStack(err))
		return
	}
	if err = m.createRaftServer(); err != nil {
		log.LogError(errors.ErrorStack(err))
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
	vfDelayCheckCrcSec := cfg.GetString(FileDelayCheckCrc)
	volMissSec := cfg.GetString(VolMissSec)
	volTimeOutSec := cfg.GetString(VolTimeOutSec)
	everyLoadVolCount := cfg.GetString(EveryLoadVolCount)
	replicaNum := cfg.GetString(ReplicaNum)
	m.walDir = cfg.GetString(WalDir)
	m.storeDir = cfg.GetString(StoreDir)
	peerAddrs := cfg.GetString(CfgPeers)
	if err = m.config.parsePeers(peerAddrs); err != nil {
		return
	}

	if m.id, err = strconv.ParseUint(cfg.GetString(ID), 10, 64); err != nil {
		return fmt.Errorf("%v,err:%v", ErrBadConfFile, err.Error())
	}

	if m.ip == "" || m.port == "" || m.walDir == "" || m.storeDir == "" || m.clusterName == "" {
		return fmt.Errorf("%v,err:%v", ErrBadConfFile, "one of (ip,port,walDir,storeDir,clusterName) is null")
	}

	if replicaNum != "" {
		if m.config.replicaNum, err = strconv.Atoi(replicaNum); err != nil {
			return fmt.Errorf("%v,err:%v", ErrBadConfFile, err.Error())
		}

		if m.config.replicaNum > 10 {
			return fmt.Errorf("%v,replicaNum(%v) can't too large", ErrBadConfFile, m.config.replicaNum)
		}
	}

	if vfDelayCheckCrcSec != "" {
		if m.config.FileDelayCheckCrcSec, err = strconv.ParseInt(vfDelayCheckCrcSec, 10, 0); err != nil {
			return fmt.Errorf("%v,err:%v", ErrBadConfFile, err.Error())
		}
	}

	if volMissSec != "" {
		if m.config.DataPartitionMissSec, err = strconv.ParseInt(volMissSec, 10, 0); err != nil {
			return fmt.Errorf("%v,err:%v", ErrBadConfFile, err.Error())
		}
	}
	if volTimeOutSec != "" {
		if m.config.DataPartitionTimeOutSec, err = strconv.ParseInt(volTimeOutSec, 10, 0); err != nil {
			return fmt.Errorf("%v,err:%v", ErrBadConfFile, err.Error())
		}
	}
	if everyLoadVolCount != "" {
		if m.config.everyLoadDataPartitionCount, err = strconv.Atoi(everyLoadVolCount); err != nil {
			return fmt.Errorf("%v,err:%v", ErrBadConfFile, err.Error())
		}
	}
	if m.config.everyLoadDataPartitionCount <= 40 {
		m.config.everyLoadDataPartitionCount = 40
	}

	return
}

func (m *Master) createRaftServer() (err error) {
	raftCfg := &raftstore.Config{NodeID: m.id, WalPath: m.walDir}
	if m.raftStore, err = raftstore.NewRaftStore(raftCfg); err != nil {
		return errors.Annotatef(err, "NewRaftStore failed! id[%v] walPath[%v]", m.id, m.walDir)
	}
	fsm := newMetadataFsm(m.storeDir)
	fsm.RegisterLeaderChangeHandler(m.handleLeaderChange)
	fsm.RegisterPeerChangeHandler(m.handlePeerChange)
	fsm.RegisterApplyHandler(m.handleApply)
	fsm.restore()
	m.fsm = fsm
	fmt.Println(m.config.peers)
	partitionCfg := &raftstore.PartitionConfig{
		ID:      GroupId,
		Peers:   m.config.peers,
		Applied: fsm.applied,
		SM:      fsm,
	}
	if m.partition, err = m.raftStore.CreatePartition(partitionCfg); err != nil {
		return errors.Annotate(err, "CreatePartition failed")
	}
	return
}
