package master

import (
	"github.com/tiglabs/baudstorage/proto"
)

type MetaNode struct {
	Addr       string
	metaRanges []*MetaRange
	isActive   bool
	sender     *AdminTaskSender
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

func (metaNode *MetaNode) dealTaskResponse(nodeAddr string, task *proto.AdminTask) {
	if task == nil {
		return
	}
	if _, ok := metaNode.sender.taskMap[task.ID]; !ok {
		return
	}

	switch task.OpCode {
	case OpCreateMetaGroup:
	default:

	}

	return

}
