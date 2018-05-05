package master

import (
	"encoding/json"
	"sync"
	"time"

	"github.com/tiglabs/baudstorage/proto"
	"net"
)

const (
	TaskSendCount = 5
	TaskSendUrl   = "/node/task"
)

/*
master send admin command to metaNode or dataNode by the sender,
because this command is cost very long time,so the sender just send command
and do nothing..then the metaNode or  dataNode send a new http request to reply command response
to master

*/
type AdminTaskSender struct {
	targetAddr string
	taskMap    map[string]*proto.AdminTask
	sync.Mutex
	exitCh chan struct{}
	conn   *net.TCPConn
}

func NewAdminTaskSender(targetAddr string) (sender *AdminTaskSender) {

	sender = &AdminTaskSender{
		targetAddr: targetAddr,
		taskMap:    make(map[string]*proto.AdminTask),
		exitCh:     make(chan struct{}),
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

//todo suggest to define a new packet protocol for send control command
func (sender *AdminTaskSender) sendTasks(tasks []*proto.AdminTask) (err error) {
	//requestBody, err := json.Marshal(tasks)
	//if err != nil {
	//	return err
	//}
	//addr := sender.targetAddr
	//_, err = util.PostToNode(requestBody, addr+TaskSendUrl)
	conn, err := net.DialTimeout("tcp", sender.targetAddr, ConnectionTimeout*time.Second)
	if err != nil {
		return
	}

	if sender.conn == nil {
		sender.conn = conn.(*net.TCPConn)
		sender.conn.SetNoDelay(true)
	}
	for _, task := range tasks {
		if err = sender.singleSend(task); err != nil {
			break
		}
	}
	sender.conn.Close()
	sender.conn = nil
	return err
}

func (sender *AdminTaskSender) singleSend(task *proto.AdminTask) (err error) {
	packet := proto.NewPacket()
	packet.Opcode = task.OpCode
	body, err := json.Marshal(task)
	if err != nil {
		return
	}
	packet.Size = uint32(len(body))
	head := make([]byte, proto.HeaderSize)
	packet.MarshalHeader(head)
	sender.conn.Write(head)
	sender.conn.Write(body)
	return
}

func (sender *AdminTaskSender) batchSend(tasks []*proto.AdminTask) (err error) {
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
