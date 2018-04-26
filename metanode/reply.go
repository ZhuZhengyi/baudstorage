package metanode

import (
	"context"
	"encoding/json"
	"github.com/juju/errors"
	"github.com/tiglabs/baudstorage/proto"
	"github.com/tiglabs/baudstorage/util"
)

var (
	ErrInvalidContext = errors.New("invalid context")
	ErrInvalidChannel = errors.New("invalid channel")
)

func (m *MetaNode) starTaskReplyService(ctx context.Context, masterReplyC chan *proto.AdminTask) (err error) {
	// Validate arguments
	if ctx == nil {
		err = ErrInvalidContext
		return
	}
	if masterReplyC == nil {
		err = ErrInvalidChannel
		return
	}
	// Start goroutine for master reply
	go func() {

	}()
	for {
		select {
		case <-ctx.Done():
			return
		case t := <-masterReplyC:
			data, err := json.Marshal([]*proto.AdminTask{t})
			if err != nil {
				continue
			}
			util.PostToNode(data, ReplyToMasterUrl)
		}
	}
	return
}
