package master

import (
	"github.com/tiglabs/baudstorage/proto"
	"sync"
	"time"
)

type MetaNode struct {
	ID                uint64
	Addr              string
	IsActive          bool
	Sender            *AdminTaskSender
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

func NewMetaNode(addr, clusterID string) (node *MetaNode) {
	return &MetaNode{
		Addr:   addr,
		Sender: NewAdminTaskSender(addr, clusterID),
	}
}

func (metaNode *MetaNode) clean() {
	metaNode.Sender.exitCh <- struct{}{}
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
	if metaNode.IsActive == true && metaNode.MaxMemAvailWeight > DefaultMinMetaPartitionRange {
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
	metaNode.IsActive = true
}

func (metaNode *MetaNode) updateMetric(resp *proto.MetaNodeHeartbeatResponse) {
	metaNode.metaRangeInfos = resp.MetaPartitionInfo
	metaNode.metaRangeCount = len(metaNode.metaRangeInfos)
	metaNode.Total = resp.Total
	metaNode.Used = resp.Used
	metaNode.MaxMemAvailWeight = resp.Total - resp.Used
	metaNode.RackName = resp.RackName
	metaNode.setNodeAlive()
}

func (metaNode *MetaNode) isArriveThreshold() bool {
	return float32(metaNode.Used/metaNode.Total) > DefaultMetaPartitionThreshold
}

func (metaNode *MetaNode) generateHeartbeatTask(masterAddr string) (task *proto.AdminTask) {
	request := &proto.HeartBeatRequest{
		CurrTime:   time.Now().Unix(),
		MasterAddr: masterAddr,
	}
	task = proto.NewAdminTask(proto.OpMetaNodeHeartbeat, metaNode.Addr, request)
	return
}
