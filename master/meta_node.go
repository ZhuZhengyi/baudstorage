package master

import (
	"github.com/tiglabs/baudstorage/proto"
	"sync"
	"time"
)

type MetaNode struct {
	id   uint64
	Addr string
	//metaPartitions    []*MetaReplica
	isActive          bool
	sender            *AdminTaskSender
	RackName          string `json:"Rack"`
	MaxMemAvailWeight uint64 `json:"MaxMemAvailWeight"`
	Total             uint64 `json:"TotalWeight"`
	Used              uint64 `json:"UsedWeight"`
	selectCount       uint64
	carry             float64
	reportTime        time.Time
	metaRangeInfos    []*proto.MetaPartitionReport
	metaRangeCount    int
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
	if metaNode.isActive == true && metaNode.MaxMemAvailWeight > DefaultMinMetaPartitionRange {
		ok = true
	}
	return
}

func (metaNode *MetaNode) IsAvailCarryNode() (ok bool) {
	metaNode.Lock()
	defer metaNode.Unlock()

	return metaNode.carry >= 1
}

func (metaNode *MetaNode) setNodeAlive() {
	metaNode.Lock()
	defer metaNode.Unlock()
	metaNode.reportTime = time.Now()
	metaNode.isActive = true
}

func (metaNode *MetaNode) generateHeartbeatTask(masterAddr string) (task *proto.AdminTask) {
	request := &proto.HeartBeatRequest{
		CurrTime:   time.Now().Unix(),
		MasterAddr: masterAddr,
	}
	task = proto.NewAdminTask(OpMetaNodeHeartbeat, metaNode.Addr, request)
	return
}
