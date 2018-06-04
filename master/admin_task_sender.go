package master

import (
	"encoding/json"
	"sync"
	"time"

	"fmt"
	"github.com/juju/errors"
	"github.com/tiglabs/baudstorage/proto"
	"github.com/tiglabs/baudstorage/util/log"
	"github.com/tiglabs/baudstorage/util/pool"
	"net"
)

const (
	MinTaskLen              = 30
	TaskWaitResponseTimeOut = time.Second * time.Duration(5)
	TaskWorkerInterval      = time.Microsecond * time.Duration(200)
)

/*
master send admin command to metaNode or dataNode by the sender,
because this command is cost very long time,so the sender just send command
and do nothing..then the metaNode or  dataNode send a new http request to reply command response
to master

*/

type AdminTaskSender struct {
	clusterID  string
	targetAddr string
	TaskMap    map[string]*proto.AdminTask
	sync.Mutex
	exitCh   chan struct{}
	connPool *pool.ConnPool
}

func NewAdminTaskSender(targetAddr, clusterID string) (sender *AdminTaskSender) {

	sender = &AdminTaskSender{
		targetAddr: targetAddr,
		clusterID:  clusterID,
		TaskMap:    make(map[string]*proto.AdminTask),
		exitCh:     make(chan struct{}),
		connPool:   pool.NewConnPool(),
	}
	go sender.process()

	return
}

func (sender *AdminTaskSender) process() {
	ticker := time.NewTicker(TaskWorkerInterval)
	defer func() {
		ticker.Stop()
		Warn(sender.clusterID, fmt.Sprintf("%v sender stop", sender.targetAddr))
	}()
	for {
		select {
		case <-sender.exitCh:
			return
		case <-ticker.C:
			sender.doDeleteTasks()
			sender.doSendTasks()
		}
	}
}

func (sender *AdminTaskSender) doDeleteTasks() {
	delTasks := sender.getNeedDeleteTasks()
	for _, t := range delTasks {
		sender.DelTask(t)
	}
	return
}

func (sender *AdminTaskSender) getNeedDeleteTasks() (delTasks []*proto.AdminTask) {
	sender.Lock()
	defer sender.Unlock()
	delTasks = make([]*proto.AdminTask, 0)
	for _, task := range sender.TaskMap {
		if task.CheckTaskTimeOut() {
			Warn(sender.clusterID, fmt.Sprintf("%v has no response util time out", task.ID))
			delTasks = append(delTasks, task)
		}
	}
	return
}

func (sender *AdminTaskSender) doSendTasks() {
	tasks := sender.getNeedDealTask()
	if len(tasks) == 0 {
		time.Sleep(time.Second)
		return
	}
	sender.sendTasks(tasks)
}

func (sender *AdminTaskSender) sendTasks(tasks []*proto.AdminTask) {

	for _, task := range tasks {
		conn, err := sender.connPool.Get(sender.targetAddr)
		if err != nil {
			msg := fmt.Sprintf("get connection to %v,err,%v", sender.targetAddr, err.Error())
			log.LogError(msg)
			Warn(sender.clusterID, msg)
			//if get connection failed,the task is sent in the next ticker
			break
		}
		if err = sender.singleSend(task, conn); err != nil {
			log.LogError(fmt.Sprintf("send task %v to %v,err,%v", task.ToString(), sender.targetAddr, err.Error()))
			continue
		}
		sender.connPool.Put(conn)
	}

}

func (sender *AdminTaskSender) buildPacket(task *proto.AdminTask) (packet *proto.Packet) {
	packet = proto.NewPacket()
	packet.Opcode = task.OpCode
	packet.ReqID = proto.GetReqID()
	body, err := json.Marshal(task)
	if err != nil {
		return
	}
	packet.Size = uint32(len(body))
	packet.Data = body
	return
}

func (sender *AdminTaskSender) singleSend(task *proto.AdminTask, conn net.Conn) (err error) {
	packet := sender.buildPacket(task)
	if err = packet.WriteToConn(conn); err != nil {
		return errors.Annotatef(err, "action[singleSend],WriteToConn failed,task:%v", task.ID)
	}
	response := proto.NewPacket()
	if err = response.ReadFromConn(conn, TaskWaitResponseTimeOut); err != nil {
		return errors.Annotatef(err, "action[singleSend],task:%v", task.ID)
	}
	if response.IsOkReply() {
		task.SendTime = time.Now().Unix()
		task.Status = proto.TaskStart
		task.SendCount++
	} else {
		log.LogErrorf("action[singleSend] send task failed,err %v", response.Data)
	}
	log.LogDebugf(fmt.Sprintf("action[singleSend] sender task:%v success", task.ToString()))

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
		log.LogDebugf("action[DelTask] delete task[%v]", t.ToString())
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

func (sender *AdminTaskSender) IsExist(t *proto.AdminTask) bool {
	sender.Lock()
	defer sender.Unlock()
	_, ok := sender.TaskMap[t.ID]
	return ok
}

func (sender *AdminTaskSender) getNeedDealTask() (tasks []*proto.AdminTask) {
	sender.Lock()
	defer sender.Unlock()
	tasks = make([]*proto.AdminTask, 0)
	for _, task := range sender.TaskMap {
		if !task.CheckTaskTimeOut() || task.CheckTaskNeedRetrySend() {
			tasks = append(tasks, task)
			continue
		}
		if len(tasks) == MinTaskLen {
			break
		}
	}
	return
}
