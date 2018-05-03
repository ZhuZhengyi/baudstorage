package raftwrapper

import (
	"fmt"
	"time"
	"errors"
	"github.com/tiglabs/raft"
	"flag"
	"github.com/tiglabs/baudstorage/util/config"
	"github.com/tiglabs/raft/proto"
	pbproto "github.com/golang/protobuf/proto"
	"github.com/volstore/src/master/protos"
	"sync"
)

var NotLeader = errors.New("not leader")

func main() {
	flag.Parse()
	var cfg *config.Config = nil //fake for test
	var s *Server

	err := StartRaftServer(s,1, cfg, RocksDBStorage)
	if err != nil{

	}
	fmt.Println("nodeid = ", s.nodeid)
}

type Server struct {
	nodeid       uint64
	addr         *Address
	resolver     *Resolver
	rs           *raft.RaftServer
	partitions   map[uint64]*partition  // 节点上的分片，key：分片ID, 一个节点上可以有多个分片
	sm           *RaftStoreFsm
	config       *Config
	dbtype       string
}

func StartRaftServer(s *Server, nodeId uint64, cfg *config.Config, dbType string) (err error) {
	//启动raft server，包含多个分片
	s.nodeid = nodeId
	s.config = NewConfig()
	s.resolver = newResolver()
	s.dbtype = dbType
	s.partitions = make(map[uint64]*partition)

	//init config
	if err := s.config.InitConfig(cfg); err != nil{
		return fmt.Errorf("raft config init failed,err:%v", err.Error())
	}

	//AddrAddr(m.id,m.ip,m.port)
	if err = AddrInit(s.config.PeerAddrs()); err != nil {
		return fmt.Errorf("raft address init failed,err:%v", err.Error())
	}

	addrInfo, ok := AddrDatabase[nodeId]
	if !ok {
		return fmt.Errorf("no such address info. master id: %d", nodeId)
	}
    s.addr = addrInfo

	//  new raft server
	if s.rs, err = newRaftServer(s.resolver, addrInfo, nodeId); err != nil {
		return fmt.Errorf("create raft server failed,err:%v", err.Error())
	}

	for _, p := range s.config.Peers() {
		s.resolver.AddNode(p.ID)
	}

	//TODO：单node上的partition个数怎么设置
	// 根据配置初始化节点上的分区
	for i := 1; i <= PartitionNum; i++ {
		s.partitions[uint64(i)], err = newPartition(s, uint64(i))
	}

	return nil
}

//Raft *db Storage Wrapper
func newRaftServer(r *Resolver, addr *Address, nodeId uint64) (rs *raft.RaftServer, err error) {

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

func (s *Server) handleLeaderChange(leader uint64) {

}

func (s *Server) handlePeerChange(confChange *proto.ConfChange) (err error) {
	return nil
}

// 提供给应用的功能，具体的kv由应用者设置
func (s *Server) RaftSubmit(raftGroupId uint64, kv *protos.Kv, lock sync.Mutex) (err error){

	if !s.rs.IsLeader(raftGroupId) {
		return fmt.Errorf("action[RaftSubmit],raftGroupId:%v,err:%v", raftGroupId, NotLeader)
	}
	var data []byte

	if data, err = pbproto.Marshal(kv); err != nil {
		err = fmt.Errorf("action[KvsmAllocateVolID],marshal kv:%v,err:%v", kv, err.Error())
		return err
	}

	lock.Lock()
	defer lock.Unlock()

	resp := s.rs.Submit(raftGroupId, data)
	if _, err = resp.Response(); err != nil {
		return fmt.Errorf("action[KvsmAllocateVolID],raft submit err:%v", err.Error())
	}

	return nil
}