package meta

import (
	"log"
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

	RefreshMetaPartitionsInterval = time.Minute * 5
	CreateInodeTimeout            = time.Second * 5

	MetaAllocBufSize = 1000
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
	namespace string
	leader    string
	master    []string
	conns     *pool.ConnPool

	// partitions and ranges should be modified together.
	// do not use partitions and ranges directly, use the helper functions instead.
	partitions map[uint64]*MetaPartition
	ranges     *btree.BTree // *MetaPartition tree indexed by Start

	currStart uint64
}

func init() {
	log.SetFlags(log.Ldate | log.Ltime | log.Lmicroseconds | log.Lshortfile)
}

func NewMetaWrapper(namespace, masterHosts string) (*MetaWrapper, error) {
	mw := new(MetaWrapper)
	mw.namespace = namespace
	mw.master = strings.Split(masterHosts, HostsSeparator)
	mw.leader = mw.master[0]
	mw.conns = pool.NewConnPool()
	mw.partitions = make(map[uint64]*MetaPartition)
	mw.ranges = btree.New(32)
	if err := mw.Update(); err != nil {
		return nil, err
	}
	mw.currStart = proto.ROOT_INO
	go mw.refresh()
	return mw, nil
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
