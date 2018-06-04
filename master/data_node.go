package master

import (
	"math/rand"
	"sync"
	"time"

	"github.com/tiglabs/baudstorage/proto"
	"github.com/tiglabs/baudstorage/util"
)

const (
	ReservedVolCount = 1
)

type DataNode struct {
	MaxDiskAvailWeight        uint64 `json:"MaxDiskAvailWeight"`
	CreatedVolWeights         uint64
	RemainWeightsForCreateVol uint64
	Total                     uint64 `json:"TotalWeight"`
	Used                      uint64 `json:"UsedWeight"`
	Free                      uint64
	RackName                  string `json:"Rack"`
	Addr                      string
	ReportTime                time.Time
	isActive                  bool
	sync.Mutex
	ratio        float64
	selectCount  uint64
	carry        float64
	sender       *AdminTaskSender
	VolInfo      []*proto.PartitionReport
	VolInfoCount uint32
}

func NewDataNode(addr, clusterID string) (dataNode *DataNode) {
	dataNode = new(DataNode)
	dataNode.carry = rand.Float64()
	dataNode.Total = 1
	dataNode.Addr = addr
	dataNode.sender = NewAdminTaskSender(dataNode.Addr, clusterID)
	return
}

/*check node heartbeat if reportTime > DataNodeTimeOut,then IsActive is false*/
func (dataNode *DataNode) checkHeartBeat() {
	dataNode.Lock()
	defer dataNode.Unlock()
	if time.Since(dataNode.ReportTime) > time.Second*time.Duration(DefaultNodeTimeOutSec) {
		dataNode.isActive = false
	}

	return
}

/*set node is online*/
func (dataNode *DataNode) setNodeAlive() {
	dataNode.Lock()
	defer dataNode.Unlock()
	dataNode.ReportTime = time.Now()
	dataNode.isActive = true
}

/*check not is offline*/
func (dataNode *DataNode) checkIsActive() bool {
	return dataNode.isActive

}

func (dataNode *DataNode) UpdateNodeMetric(resp *proto.DataNodeHeartBeatResponse) {
	dataNode.Lock()
	defer dataNode.Unlock()
	dataNode.MaxDiskAvailWeight = resp.MaxWeightsForCreateVol
	dataNode.CreatedVolWeights = resp.CreatedVolWeights
	dataNode.RemainWeightsForCreateVol = resp.RemainWeightsForCreateVol
	dataNode.Total = resp.Total
	dataNode.Used = resp.Used
	dataNode.Free = resp.Free
	dataNode.RackName = resp.RackName
	dataNode.VolInfoCount = resp.CreatedVolCnt
	dataNode.VolInfo = resp.PartitionInfo
	dataNode.ratio = (float64)(dataNode.Used) / (float64)(dataNode.Total)
	dataNode.ReportTime = time.Now()
}

func (dataNode *DataNode) IsWriteAble() (ok bool) {
	dataNode.Lock()
	defer dataNode.Unlock()

	if dataNode.isActive == true && dataNode.MaxDiskAvailWeight > (uint64)(util.DefaultVolSize) &&
		dataNode.Total-dataNode.Used > (uint64)(util.DefaultVolSize)*ReservedVolCount {
		ok = true
	}

	return
}

func (dataNode *DataNode) IsAvailCarryNode() (ok bool) {
	dataNode.Lock()
	defer dataNode.Unlock()

	return dataNode.carry >= 1
}

func (dataNode *DataNode) SetCarry(carry float64) {
	dataNode.Lock()
	defer dataNode.Unlock()
	dataNode.carry = carry
}

func (dataNode *DataNode) SelectNodeForWrite() {
	dataNode.Lock()
	defer dataNode.Unlock()
	dataNode.ratio = float64(dataNode.Used) / float64(dataNode.Total)
	dataNode.selectCount++
	dataNode.Used += (uint64)(util.DefaultVolSize)
	dataNode.carry = dataNode.carry - 1.0
}

func (dataNode *DataNode) clean() {
	dataNode.sender.exitCh <- struct{}{}
}

func (dataNode *DataNode) generateHeartbeatTask(masterAddr string) (task *proto.AdminTask) {
	request := &proto.HeartBeatRequest{
		CurrTime:   time.Now().Unix(),
		MasterAddr: masterAddr,
	}
	task = proto.NewAdminTask(proto.OpDataNodeHeartbeat, dataNode.Addr, request)
	return
}
