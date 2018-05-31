package meta

import (
	"strings"
	"sync"
	"time"

	"github.com/google/btree"

	"github.com/tiglabs/baudstorage/proto"
	"github.com/tiglabs/baudstorage/util/pool"
)

const (
	HostsSeparator       = ","
	MetaPartitionViewURL = "/client/namespace?name="
	GetClusterInfoURL    = "/admin/getIp"

	RefreshMetaPartitionsInterval = time.Minute * 5
)

const (
	statusOK int = iota
	statusExist
	statusNoent
	statusFull
	statusUnknownError
)

type MetaWrapper struct {
	sync.RWMutex
	cluster   string
	namespace string
	leader    string
	master    []string
	conns     *pool.ConnPool

	// Partitions and ranges should be modified together. So do not
	// use partitions and ranges directly. Use the helper functions instead.

	// Partition map indexed by ID
	partitions map[uint64]*MetaPartition

	// Partition tree indexed by Start, in order to find a partition in which
	// a specific inode locate.
	ranges *btree.BTree

	currStart uint64
}

func NewMetaWrapper(namespace, masterHosts string) (*MetaWrapper, error) {
	mw := new(MetaWrapper)
	mw.namespace = namespace
	mw.master = strings.Split(masterHosts, HostsSeparator)
	mw.leader = mw.master[0]
	mw.conns = pool.NewConnPool()
	mw.partitions = make(map[uint64]*MetaPartition)
	mw.ranges = btree.New(32)
	mw.UpdateClusterInfo()
	if err := mw.UpdateMetaPartitions(); err != nil {
		return nil, err
	}
	mw.currStart = proto.ROOT_INO
	go mw.refresh()
	return mw, nil
}

func (mw *MetaWrapper) Cluster() string {
	return mw.cluster
}

// Status code conversion
func parseStatus(status uint8) (ret int) {
	switch status {
	case proto.OpOk:
		ret = statusOK
	case proto.OpExistErr:
		ret = statusExist
	case proto.OpNotExistErr:
		ret = statusNoent
	case proto.OpInodeFullErr:
		ret = statusFull
	default:
		ret = statusUnknownError
	}
	return
}
