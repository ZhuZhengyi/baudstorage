package multiraft

import (
	"fmt"
	pbProto "github.com/golang/protobuf/proto"
	"github.com/tiglabs/raft"
	"sync"
)

// State:
//  +-----+             +-------+
//  | New | → Restore → | Ready |
//  +-----+             +-------+
type Partition interface {
	Submit(kv *Kv, mu sync.Mutex) error
}

type partition struct {
	id uint64
	rs *raft.RaftServer

}

// 提供给应用的功能，具体的kv由应用者设置
func (p *partition) Submit(kv *Kv, lock sync.Mutex) (err error) {

	if !p.rs.IsLeader(p.id) {
		return fmt.Errorf("action[RaftSubmit],raftGroupId:%v,err:%v", p.id, NotLeader)
	}
	var data []byte
	if data, err = pbProto.Marshal(kv); err != nil {
		err = fmt.Errorf("action[KvsmAllocateVolID],marshal kv:%v,err:%v", kv, err.Error())
		return err
	}

	lock.Lock()
	defer lock.Unlock()

	resp := p.rs.Submit(p.id, data)
	if _, err = resp.Response(); err != nil {
		return fmt.Errorf("action[KvsmAllocateVolID],raft submit err:%v", err.Error())
	}
	return nil
}

func NewPartition(id uint64, rs *raft.RaftServer) Partition {
	return &partition{
		id: id,
		rs: rs,
	}
}