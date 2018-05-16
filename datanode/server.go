package datanode

import (
	"github.com/juju/errors"
	"github.com/tiglabs/baudstorage/util/config"
	"github.com/tiglabs/baudstorage/util/pool"
)

var (
	ErrStoreTypeUnmatch   = errors.New("store type error")
	ErrVolNotExist        = errors.New("vol not exsits")
	ErrChunkOffsetUnmatch = errors.New("chunk offset not unmatch")
)

type DataNode struct {
	ConnPool *pool.ConnPool
	space    *SpaceManager
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

func (s *DataNode) AddCompactTask(t *CompactTask) (err error) {
	v := s.space.getVol(t.volId)
	if v == nil {
		return nil
	}
	d, _ := s.space.getDisk(v.path)
	if d == nil {
		return nil
	}
	err = d.addTask(t)
	if err != nil {
		err = errors.Annotatef(err, "Task[%v] ", t.toString())
	}

	return
}
