package master

import (
	"encoding/json"
	"sync"
	"time"

	"github.com/tiglabs/baudstorage/proto"
	"github.com/tiglabs/baudstorage/util"
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
}

func NewAdminTaskSender(targetAddr string) (sender *AdminTaskSender) {

	sender = &AdminTaskSender{
		targetAddr: targetAddr,
		taskMap:    make(map[string]*proto.AdminTask),
		exitCh:     make(chan struct{}),
	}
	go sender.send()

	return
}

func (sender *AdminTaskSender) send() {
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

func (sender *AdminTaskSender) sendTasks(tasks []*proto.AdminTask) error {
	requestBody, err := json.Marshal(tasks)
	if err != nil {
		return err
	}
	addr := sender.targetAddr
	_, err = util.PostToNode(requestBody, addr+TaskSendUrl)
	return err
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
