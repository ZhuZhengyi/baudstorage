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

type AdminTaskSender struct {
	targetAddr string
	TaskMap    map[string]*proto.AdminTask
	sync.Mutex
	exitCh   chan struct{}
	connPool *pool.ConnPool
}

func NewAdminTaskSender(targetAddr string) (sender *AdminTaskSender) {

	sender = &AdminTaskSender{
		targetAddr: targetAddr,
		TaskMap:    make(map[string]*proto.AdminTask),
		exitCh:     make(chan struct{}),
		connPool:   pool.NewConnPool(),
	}
	go sender.process()

	return
}

func (sender *AdminTaskSender) process() {
	ticker := time.NewTicker(TaskWorkerInterval)
	for {
		select {
		case <-sender.exitCh:
			return
		case <-ticker.C:
			tasks := sender.getNeedDealTask()
			if len(tasks) == 0 {
				time.Sleep(time.Second)
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
			//if get connection failed,the task is sent in the next ticker
			break
		}
		if err = sender.singleSend(task, conn); err != nil {
			log.LogError(fmt.Sprintf("send task %v to %v,err,%v", task.ToString(), sender.targetAddr, err.Error()))
			continue
		}
	}

}

func (sender *AdminTaskSender) buildPacket(task *proto.AdminTask) (packet *proto.Packet) {
	packet = proto.NewPacket()
	packet.Opcode = task.OpCode
	body, err := json.Marshal(task)
	if err != nil {
		return
	}
	packet.Size = uint32(len(body))
	packet.Data = body
	return
}

func (sender *AdminTaskSender) singleSend(task *proto.AdminTask, conn net.Conn) (err error) {
	log.LogDebugf("task info[%v]", task.ToString())
	packet := sender.buildPacket(task)
	if err = packet.WriteToConn(conn); err != nil {
		return
	}
	log.LogDebugf("send task success[%v]", task.ToString())
	response := proto.NewPacket()
	if err = response.ReadFromConn(conn, TaskWaitResponseTimeOut); err != nil {
		return
	}
	if response.IsOkReply() {
		task.SendTime = time.Now().Unix()
		task.Status = proto.TaskStart
		task.SendCount++
	} else {
		log.LogError("send task failed,err %v", response.Data)
	}
	log.LogDebugf(fmt.Sprintf("sender task:%v to %v,send time:%v,sendCount:%v,status:%v ", task.ID, sender.targetAddr, task.SendTime, task.SendCount, task.Status))
	return
}

func (sender *AdminTaskSender) DelTask(t *proto.AdminTask) {
	sender.Lock()
	defer sender.Unlock()
	_, ok := sender.TaskMap[t.ID]
	if !ok {
		return
	}
	if t.OpCode != proto.OpMetaNodeHeartbeat && t.OpCode != proto.OpDataNodeHeartbeat {
		log.LogDebugf("delete task[%v]", t.ToString())
	}
	delete(sender.TaskMap, t.ID)
}

func (sender *AdminTaskSender) PutTask(t *proto.AdminTask) {
	sender.Lock()
	defer sender.Unlock()
	_, ok := sender.TaskMap[t.ID]
	if !ok {
		sender.TaskMap[t.ID] = t
	}
}

func (sender *AdminTaskSender) getNeedDealTask() (tasks []*proto.AdminTask) {
	tasks = make([]*proto.AdminTask, 0)
	delTasks := make([]*proto.AdminTask, 0)
	sender.Lock()
	defer sender.Unlock()
	for _, task := range sender.TaskMap {
		if task.CheckTaskTimeOut() {
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

	for _, t := range delTasks {
		sender.DelTask(t)
	}

	return
}
