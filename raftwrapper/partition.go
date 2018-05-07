package raftwrapper

import (
	"fmt"
	"github.com/tiglabs/raft"
	"github.com/tiglabs/raft/storage/wal"
)

type partition struct {
	id uint64
	sm *RaftStoreFsm
	rs *raft.RaftServer
}

//由应用者调用。需要提供s.rs，s.config，groupid
func NewRaftPartition(s *Server, partitionId uint64) (p *partition, err error) {
	p = &partition{
		id: partitionId,
		sm: NewRaftStateMachine(s.nodeid, partitionId, s.config.StoreDir(), s.dbtype, s.rs),
		rs: s.rs,
	}

	wc := &wal.Config{}
	raftStorage, err := wal.NewStorage(fmt.Sprintf(s.config.WalDir() + "/wal%d", partitionId), wc)
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
