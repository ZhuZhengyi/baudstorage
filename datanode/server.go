package datanode

import (
	"github.com/tiglabs/baudstorage/util/config"
)

type DataNode struct {
}

func (n *DataNode) Start(cfg *config.Config) error {
	panic("implement me")
}

func (n *DataNode) Shutdown() {
	panic("implement me")
}

func (n *DataNode) Sync() {
	panic("implement me")
}

func NewServer() *DataNode {
	return &DataNode{}
}
