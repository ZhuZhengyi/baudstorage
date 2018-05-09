package multiraft

import (
	"errors"
	"fmt"
	"github.com/tiglabs/raft"
	"github.com/tiglabs/raft/proto"
	"github.com/tiglabs/raft/storage/wal"
	"time"
)

var NotLeader = errors.New("not leader")

type MultiRaftServer struct {
	nodeId     uint64
	addr       *Address
	resolver   *Resolver
	rs         *raft.RaftServer
	partitions map[uint64]*RaftPartition // 节点上的分片，key：分片ID, 一个节点上可以有多个分片
	config     *Config
	dbtype     string
}

func NewMultiRaftServer(nodeId uint64, cfg *Config, dbType string) (server *MultiRaftServer, err error) {
	// Init address
	if err = AddrInit(cfg.PeerAddrs()); err != nil {
		return
	}
	// Init address info
	addrInfo, ok := AddrDatabase[nodeId]
	if !ok {
		err = errors.New(fmt.Sprintf("no such address info. master id: %d", nodeId))
		return
	}
	// Init resolver.
	resolver := newResolver()
	for _, peer := range server.config.peers {
		resolver.AddNode(peer.ID)
	}
	// Init raft server.
	raftServer, err := initRaftServer(resolver, addrInfo, nodeId)
	if err != nil {
		return
	}
	// Init multi raft server instance.
	server = &MultiRaftServer{
		nodeId:     nodeId,
		config:     cfg,
		resolver:   resolver,
		dbtype:     dbType,
		partitions: make(map[uint64]*RaftPartition),
		addr:       addrInfo,
		rs:         raftServer,
	}
	return
}

//Raft *db Storage Wrapper
func initRaftServer(r *Resolver, addr *Address, nodeId uint64) (rs *raft.RaftServer, err error) {

	//  new raft_store server
	c := raft.DefaultConfig()
	c.TickInterval = time.Millisecond * 200
	c.NodeID = nodeId
	c.Resolver = r
	c.HeartbeatAddr = addr.Heartbeat
	c.ReplicateAddr = addr.Replicate
	c.RetainLogs = TruncateInterval
	rs, err = raft.NewRaftServer(c)
	if err != nil {
		err = fmt.Errorf("actoin[CreateRaftServerErr],err:%v", err.Error())
		return nil, err
	}
	return rs, nil
}

func (s *MultiRaftServer) handleLeaderChange(leader uint64) {

}

func (s *MultiRaftServer) handlePeerChange(confChange *proto.ConfChange) (err error) {
	return nil
}

func (s *MultiRaftServer) NewPartition(partitionId uint64) (p *RaftPartition, err error) {
	p = &RaftPartition{
		id: partitionId,
		sm: NewRaftStateMachine(s.nodeId, partitionId, s.config.StoreDir(), s.dbtype, s.rs),
		rs: s.rs,
	}

	wc := &wal.Config{}
	raftStorage, err := wal.NewStorage(fmt.Sprintf(s.config.WalDir()+"/wal%d", partitionId), wc)
	if err != nil {
		return nil, err
	}

	// state machine
	p.sm.Restore()
	p.sm.RegisterLeaderChangeHandler(s.handleLeaderChange)
	p.sm.RegisterPeerChangeHandler(s.handlePeerChange)

	rc := &raft.RaftConfig{
		ID:           partitionId,
		Peers:        s.config.Peers(),
		Storage:      raftStorage,
		StateMachine: p.sm,
		Applied:      p.sm.GetApplied(),
	}
	if err = s.rs.CreateRaft(rc); err != nil {
		return nil, fmt.Errorf("create raft failed,err:%v", err.Error())
	}

	return p, nil
}
