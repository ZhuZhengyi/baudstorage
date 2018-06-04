package data

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"math/rand"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/tiglabs/baudstorage/proto"
	"github.com/tiglabs/baudstorage/storage"
	"github.com/tiglabs/baudstorage/util/log"
	"github.com/tiglabs/baudstorage/util/pool"
)

const (
	DataPartitionViewUrl       = "/client/datapartition?name="
	ActionGetDataPartitionView = "ActionGetDataPartitionView"
)

const (
	MinWritableDataPartitionNum = 20
)

type DataPartition struct {
	DataPartitionID uint32
	Status          uint8
	ReplicaNum      uint8
	Hosts           []string
}

type DataPartitionView struct {
	Partitions []*DataPartition
}

func (dp *DataPartition) GetAllAddrs() (m string) {
	return strings.Join(dp.Hosts[1:], proto.AddrSplit) + proto.AddrSplit
}

type DataPartitionWrapper struct {
	sync.RWMutex
	namespace   string
	master      []string
	conns       *pool.ConnPool
	partitions  map[uint32]*DataPartition
	rwPartition []*DataPartition
}

func NewDataPartitionWrapper(namespace, masterHosts string) (wrapper *DataPartitionWrapper, err error) {
	master := strings.Split(masterHosts, ",")
	wrapper = new(DataPartitionWrapper)
	wrapper.master = master
	wrapper.namespace = namespace
	wrapper.conns = pool.NewConnPool()
	wrapper.rwPartition = make([]*DataPartition, 0)
	wrapper.partitions = make(map[uint32]*DataPartition)
	if err = wrapper.getDataPartitionFromMaster(); err != nil {
		return
	}
	go wrapper.update()
	return
}

func (wrapper *DataPartitionWrapper) update() {
	ticker := time.NewTicker(time.Minute * 5)
	for {
		select {
		case <-ticker.C:
			wrapper.getDataPartitionFromMaster()
		}
	}
}

func (wrapper *DataPartitionWrapper) getDataPartitionFromMaster() (err error) {
	for _, m := range wrapper.master {
		if m == "" {
			continue
		}
		var resp *http.Response
		resp, err = http.Get("http://" + m + DataPartitionViewUrl + wrapper.namespace)
		if err != nil {
			log.LogError(fmt.Sprintf(ActionGetDataPartitionView+"get VolView from master[%v] err[%v]", m, err.Error()))
			continue
		}
		defer resp.Body.Close()
		body, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			continue
		}
		views := new(DataPartitionView)
		if err = json.Unmarshal(body, views); err != nil {
			log.LogError(fmt.Sprintf(ActionGetDataPartitionView+"get VolView from master[%v] err[%v]", m, err.Error()))
			continue
		}
		log.LogInfof("Get VolView from master: %v", string(body))
		wrapper.updateDataPartition(views.Partitions)
		break
	}

	return
}

func (wrapper *DataPartitionWrapper) replaceOrInsertPartition(dp *DataPartition) {
	wrapper.Lock()
	defer wrapper.Unlock()
	if _, ok := wrapper.partitions[dp.DataPartitionID]; ok {
		delete(wrapper.partitions, dp.DataPartitionID)
	}
	wrapper.partitions[dp.DataPartitionID] = dp
}

func (wrapper *DataPartitionWrapper) updateDataPartition(partitions []*DataPartition) {
	rwPartitionGroups := make([]*DataPartition, 0)
	for _, dp := range partitions {
		if dp.Status == storage.ReadWriteStore {
			rwPartitionGroups = append(rwPartitionGroups, dp)
		}
	}

	// If the view received from master cannot guarentee the minimum number
	// of volume partitions, it is probably due to a **temporary** network problem
	// between master and datanode. So do not update the vol group view for
	// now, and use the old information.
	if len(rwPartitionGroups) < MinWritableDataPartitionNum {
		return
	}
	wrapper.rwPartition = rwPartitionGroups

	for _, dp := range partitions {
		wrapper.replaceOrInsertPartition(dp)
	}
}

func isExcluded(partitionId uint32, excludes []uint32) bool {
	for _, id := range excludes {
		if id == partitionId {
			return true
		}
	}
	return false
}

func (wrapper *DataPartitionWrapper) GetWriteDataPartition(exclude []uint32) (*DataPartition, error) {
	rwPartitionGroups := wrapper.rwPartition
	if len(rwPartitionGroups) == 0 {
		return nil, fmt.Errorf("No writable DataPartition")
	}

	rand.Seed(time.Now().UnixNano())
	choose := rand.Intn(len(rwPartitionGroups))
	partition := rwPartitionGroups[choose]
	if !isExcluded(partition.DataPartitionID, exclude) {
		return partition, nil
	}

	for _, partition = range rwPartitionGroups {
		if !isExcluded(partition.DataPartitionID, exclude) {
			return partition, nil
		}
	}
	return nil, fmt.Errorf("No writable DataPartition")
}

func (wrapper *DataPartitionWrapper) GetDataPartition(partitionID uint32) (*DataPartition, error) {
	wrapper.RLock()
	defer wrapper.RUnlock()
	dp, ok := wrapper.partitions[partitionID]
	if !ok {
		return nil, fmt.Errorf("DataPartition[%v] not exsit", partitionID)
	}
	return dp, nil
}

func (wrapper *DataPartitionWrapper) GetConnect(addr string) (net.Conn, error) {
	return wrapper.conns.Get(addr)
}

func (wrapper *DataPartitionWrapper) PutConnect(conn net.Conn) {
	wrapper.conns.Put(conn)
}
