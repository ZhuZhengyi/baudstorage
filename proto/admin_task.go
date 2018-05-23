package proto

import (
	"fmt"
	"time"
)

const (
	TaskFail         = 2
	TaskStart        = 0
	TaskSuccess      = 1
	ResponseInterval = 30
	ResponseTimeOut  = 100
	MaxSendCount     = 5
	CreateVol        = iota
	LoadVol
	CreateMetaRange
	GetMetaRangeMetric
	HeartBeat
)

/*task struct to node*/
type AdminTask struct {
	ID           string
	OpCode       uint8
	OperatorAddr string
	Status       int8
	SendTime     int64
	SendCount    uint8
	Request      interface{}
	Response     interface{}
}

func (t *AdminTask) ToString() (msg string) {
	msg = fmt.Sprintf("FileIdId[%v] Status[%d] LastSendTime[%v]  SendCount[%v] ",
		t.ID, t.Status, t.SendTime, t.SendCount)

	return
}

/*check task need retry send if task ResponseTimeOut then
need retry to node,if task is TaskRunning and task is OpReplicateFile
cannot retry send it to node*/
func (t *AdminTask) CheckTaskNeedRetrySend() (needRetry bool) {
	if time.Now().Unix()-t.SendTime > (int64)(ResponseInterval) && t.Status == TaskStart {
		needRetry = true
	}
	return
}

func (t *AdminTask) CheckTaskTimeOut() (notResponse bool) {
	var (
		timeOut int64
	)
	timeOut = ResponseTimeOut
	if (int)(t.SendCount) >= MaxSendCount || (time.Now().Unix()-t.SendTime > timeOut && t.Status != TaskStart) {
		notResponse = true
	}

	return
}

func (t *AdminTask) CheckTaskIsSuccess() (isSuccess bool) {
	if t.Status == TaskSuccess {
		isSuccess = true
	}

	return
}

func (t *AdminTask) CheckTaskIsFail() (isFail bool) {
	if t.Status == TaskFail {
		isFail = true
	}

	return
}

func NewAdminTask(opcode uint8, opAddr string, request interface{}) (t *AdminTask) {
	t = new(AdminTask)
	t.ID = fmt.Sprintf("addr[%v]_op[%v]", t.OperatorAddr, t.OpCode)
	t.OpCode = opcode
	t.Request = request
	t.OperatorAddr = opAddr

	return
}
