package master

import (
	"encoding/json"
	"sync"
	"time"

	"fmt"
	"github.com/tiglabs/baudstorage/proto"
	"github.com/tiglabs/baudstorage/util/log"
	"github.com/tiglabs/baudstorage/util/pool"
	"net"
)

const (
	TaskSendCount           = 5
	TaskWaitResponseTimeOut = time.Second * time.Duration(3)
)

/*
master send admin command to metaNode or dataNode by the sender,
because this command is cost very long time,so the sender just send command
and do nothing..then the metaNode or  dataNode send a new http request to reply command response
to master

*/
type CommandRequest struct {
	head []byte
	body []byte
}

func NewCommandRequest() (req *CommandRequest) {
	req = &CommandRequest{
		head: make([]byte, proto.HeaderSize),
		body: make([]byte, 0),
	}
	return
}

func (cr *CommandRequest) setHeadAndBody(task *proto.AdminTask) (err error) {
	packet := proto.NewPacket()
	packet.Opcode = task.OpCode
	body, err := json.Marshal(task)
	if err != nil {
		return
	}
	packet.Size = uint32(len(body))
	packet.MarshalHeader(cr.head)
	cr.body = body
	return
}

type AdminTaskSender struct {
	targetAddr string
	taskMap    map[string]*proto.AdminTask
	sync.Mutex
	exitCh   chan struct{}
	connPool *pool.ConnPool
}

func NewAdminTaskSender(targetAddr string) (sender *AdminTaskSender) {

	sender = &AdminTaskSender{
		targetAddr: targetAddr,
		taskMap:    make(map[string]*proto.AdminTask),
		exitCh:     make(chan struct{}),
		connPool:   pool.NewConnPool(),
	}
	go sender.process()

	return
}

func (sender *AdminTaskSender) process() {
	ticker := time.Tick(time.Second)
	for {
		select {
		case <-sender.exitCh:
			return
		case <-ticker:
			time.Sleep(time.Millisecond * 100)
		default:
			tasks := sender.getNeedDealTask()
			if len(tasks) == 0 {
				time.Sleep(time.Millisecond * 100)
				continue
			}
			sender.sendTasks(tasks)

		}
	}

}

func (sender *AdminTaskSender) sendTasks(tasks []*proto.AdminTask) {

	for _, task := range tasks {
		conn, err := sender.connPool.Get(sender.targetAddr)
		if err != nil {
			log.LogError(fmt.Sprintf("get connection to %v,err,%v", sender.targetAddr, err.Error()))
			continue
		}
		if err = sender.singleSend(task, conn); err != nil {
			log.LogError(fmt.Sprintf("send task %v to %v,err,%v", task.ToString(), sender.targetAddr, err.Error()))
			continue
		}
	}

}

func (sender *AdminTaskSender) singleSend(task *proto.AdminTask, conn net.Conn) (err error) {
	cr := NewCommandRequest()
	if err = cr.setHeadAndBody(task); err != nil {
		return
	}
	if _, err = conn.Write(cr.head); err != nil {
		return
	}

	if _, err = conn.Write(cr.body); err != nil {
		return
	}
	response := proto.NewPacket()
	if err = response.ReadFromConn(conn, TaskWaitResponseTimeOut); err != nil {
		return
	}
	if response.Opcode == proto.OpOk {
		task.SendTime = time.Now().Unix()
		task.SendCount++
	}
	return
}

func (sender *AdminTaskSender) DelTask(t *proto.AdminTask) {
	sender.Lock()
	defer sender.Unlock()
	_, ok := sender.taskMap[t.ID]
	if !ok {
		return
	}
	delete(sender.taskMap, t.ID)
}

func (sender *AdminTaskSender) PutTask(t *proto.AdminTask) {
	sender.Lock()
	defer sender.Unlock()
	_, ok := sender.taskMap[t.ID]
	if !ok {
		sender.taskMap[t.ID] = t
	}
}

func (sender *AdminTaskSender) getNeedDealTask() (tasks []*proto.AdminTask) {
	tasks = make([]*proto.AdminTask, 0)
	delTasks := make([]*proto.AdminTask, 0)
	sender.Lock()
	defer sender.Unlock()
	for _, task := range sender.taskMap {
		if task.Status == proto.TaskFail || task.Status == proto.TaskSuccess || task.CheckTaskTimeOut() {
			delTasks = append(delTasks, task)
		}
		if !task.CheckTaskNeedRetrySend() {
			continue
		}
		tasks = append(tasks, task)
		if len(tasks) == TaskSendCount {
			break
		}
	}

	for _, delTask := range delTasks {
		delete(sender.taskMap, delTask.ID)
	}

	return
}
