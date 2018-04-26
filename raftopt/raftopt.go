package raftopt

import (
	"fmt"
	"github.com/tiglabs/raft"
	"time"
)

//CreateRaftServer ...
func CreateRaftServer(r *Resolver, addr *Address, nodeId uint64) (rs *raft.RaftServer, err error) {

	//  new raft_store server
	c := raft.DefaultConfig()
	c.TickInterval = time.Millisecond * 200
	c.NodeID = nodeId
	c.Resolver = r
	c.HeartbeatAddr = addr.Heartbeat
	c.ReplicateAddr = addr.Replicate
	//c.RetainLogs = TruncateInterval
	rs, err = raft.NewRaftServer(c)
	if err != nil {
		err = fmt.Errorf("actoin[CreateRaftServerErr],err:%v", err.Error())
		return nil, err
	}
	//log.InitFileLog("/export/log/hh","","DEBUG")
	return rs, nil
}
