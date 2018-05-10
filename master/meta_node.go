package master

import (
	"github.com/tiglabs/baudstorage/proto"
	"time"
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

func (metaNode *MetaNode) generateHeartbeatTask() (task *proto.AdminTask) {
	request := &proto.HeartBeatRequest{
		CurrTime: time.Now().Unix(),
	}
	task = proto.NewAdminTask(OpMetaNodeHeartbeat, metaNode.Addr, request)
	return
}
