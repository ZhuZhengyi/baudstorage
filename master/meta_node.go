package master

import (
	"github.com/tiglabs/baudstorage/proto"
	"sync"
	"time"
)

type MetaNode struct {
	id                uint64
	Addr              string
	metaRanges        []*MetaRange
	isActive          bool
	sender            *AdminTaskSender
	RackName          string `json:"Rack"`
	MaxMemAvailWeight uint64 `json:"MaxMemAvailWeight"`
	Total             uint64 `json:"TotalWeight"`
	Used              uint64 `json:"UsedWeight"`
	selectCount       uint64
	carry             float64
	sync.Mutex
}

func NewMetaNode(addr string) (node *MetaNode) {
	return &MetaNode{
		Addr:   addr,
		sender: NewAdminTaskSender(addr),
	}
}

func (metaNode *MetaNode) clean() {
	metaNode.sender.exitCh <- struct{}{}
}

func (metaNode *MetaNode) SetCarry(carry float64) {
	metaNode.Lock()
	defer metaNode.Unlock()
	metaNode.carry = carry
}

func (metaNode *MetaNode) SelectNodeForWrite() {
	metaNode.Lock()
	defer metaNode.Unlock()
	metaNode.selectCount++
	metaNode.carry = metaNode.carry - 1.0
}

func (metaNode *MetaNode) IsWriteAble() (ok bool) {
	metaNode.Lock()
	defer metaNode.Unlock()
	if metaNode.isActive == true && metaNode.MaxMemAvailWeight > DefaultMinMetaRangeSize {
		ok = true
	}
	return
}

func (metaNode *MetaNode) IsAvailCarryNode() (ok bool) {
	metaNode.Lock()
	defer metaNode.Unlock()

	return metaNode.carry >= 1
}

func (metaNode *MetaNode) generateHeartbeatTask() (task *proto.AdminTask) {
	request := &proto.HeartBeatRequest{
		CurrTime: time.Now().Unix(),
	}
	task = proto.NewAdminTask(OpMetaNodeHeartbeat, metaNode.Addr, request)
	return
}
