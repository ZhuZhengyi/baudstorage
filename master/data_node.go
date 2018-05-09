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
	TcpAddr            string `json:"TcpAddr"`
	MaxDiskAvailWeight uint64 `json:"MaxDiskAvailWeight"`
	Total              uint64 `json:"TotalWeight"`
	Used               uint64 `json:"UsedWeight"`
	ZoneName           string `json:"Zone"`
	HttpAddr           string

	reportTime time.Time
	isActive   bool
	sync.Mutex
	ratio        float64
	selectCount  uint64
	carry        float64
	sender       *AdminTaskSender
	VolInfo      []*VolReport
	VolInfoCount int
}

func NewDataNode(addr string) (dataNode *DataNode) {
	dataNode = new(DataNode)
	dataNode.carry = rand.Float64()
	dataNode.Total = 1
	dataNode.sender = NewAdminTaskSender(dataNode.HttpAddr)
	dataNode.HttpAddr = addr
	return
}

/*check node heartbeat if reportTime > DataNodeTimeOut,then isActive is false*/
func (dataNode *DataNode) checkHeartBeat() {
	dataNode.Lock()
	defer dataNode.Unlock()
	//if time.Since(dataNode.reportTime) > time.Second*(time.Duration(gConfig.NodeTimeOutSec)) {
	//	dataNode.isActive = false
	//}

	return
}

/*set node is online*/
func (dataNode *DataNode) setNodeAlive() {
	dataNode.Lock()
	defer dataNode.Unlock()
	dataNode.reportTime = time.Now()
	dataNode.isActive = true
}

/*check not is offline*/
func (dataNode *DataNode) checkIsActive() bool {
	dataNode.Lock()
	defer dataNode.Unlock()
	return dataNode.isActive

}

func (dataNode *DataNode) UpdateNodeMetric(sourceNode *DataNode) {
	dataNode.Lock()
	defer dataNode.Unlock()
	dataNode.MaxDiskAvailWeight = sourceNode.MaxDiskAvailWeight
	dataNode.Total = sourceNode.Total
	dataNode.Used = sourceNode.Used
	dataNode.ZoneName = sourceNode.ZoneName
	dataNode.ratio = (float64)(dataNode.Used) / (float64)(dataNode.Total)
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

func (dataNode *DataNode) generateHeartbeatTask() (task *proto.AdminTask) {
	request := &proto.HeartBeatRequest{
		CurrTime: time.Now().Unix(),
	}
	task = proto.NewAdminTask(OpDataNodeHeartbeat, dataNode.HttpAddr, request)
	return
}
