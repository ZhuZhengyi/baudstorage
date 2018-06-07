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
	DataPartitionViewUrl       = "/client/dataPartitions?name="
	ActionGetDataPartitionView = "ActionGetDataPartitionView"
)

const (
	MinWritableDataPartitionNum = 1 //FIXME
)

type DataPartition struct {
	PartitionID   uint32
	Status        uint8
	ReplicaNum    uint8
	PartitionType string
	Hosts         []string
}

type DataPartitionView struct {
	DataPartitions []*DataPartition
}

func (dp *DataPartition) String() string {
	return fmt.Sprintf("PartitionID(%v) Status(%v) ReplicaNum(%v) PartitionType(%v) Hosts(%v)", dp.PartitionID, dp.Status, dp.ReplicaNum, dp.PartitionType, dp.Hosts)
}

func (dp *DataPartition) GetAllAddrs() (m string) {
	return strings.Join(dp.Hosts[1:], proto.AddrSplit) + proto.AddrSplit
}

type Wrapper struct {
	sync.RWMutex
	namespace   string
	master      []string
	conns       *pool.ConnPool
	partitions  map[uint32]*DataPartition
	rwPartition []*DataPartition
}

func NewDataPartitionWrapper(namespace, masterHosts string) (w *Wrapper, err error) {
	master := strings.Split(masterHosts, ",")
	w = new(Wrapper)
	w.master = master
	w.namespace = namespace
	w.conns = pool.NewConnPool()
	w.rwPartition = make([]*DataPartition, 0)
	w.partitions = make(map[uint32]*DataPartition)
	if err = w.getDataPartitionFromMaster(); err != nil {
		return
	}
	go w.update()
	return
}

func (w *Wrapper) update() {
	ticker := time.NewTicker(time.Minute * 5)
	for {
		select {
		case <-ticker.C:
			w.getDataPartitionFromMaster()
		}
	}
}

func (w *Wrapper) getDataPartitionFromMaster() (err error) {
	for _, m := range w.master {
		if m == "" {
			continue
		}
		var resp *http.Response
		resp, err = http.Get("http://" + m + DataPartitionViewUrl + w.namespace)
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
			log.LogError(fmt.Sprintf(ActionGetDataPartitionView+"get VolView from master[%v] err[%v] body(%v)", m, err.Error(), string(body)))
			continue
		}
		log.LogInfof("Get DataPartitions from master: (%v)", *views)
		w.updateDataPartition(views.DataPartitions)
		break
	}

	return
}

func (w *Wrapper) replaceOrInsertPartition(dp *DataPartition) {
	w.Lock()
	defer w.Unlock()
	if _, ok := w.partitions[dp.PartitionID]; ok {
		delete(w.partitions, dp.PartitionID)
	}
	w.partitions[dp.PartitionID] = dp
}

func (w *Wrapper) updateDataPartition(partitions []*DataPartition) {
	rwPartitionGroups := make([]*DataPartition, 0)
	for _, dp := range partitions {
		//log.LogInfof("Get dp(%v)", dp)
		if dp.Status == storage.ReadWriteStore {
			rwPartitionGroups = append(rwPartitionGroups, dp)
		}
	}

	// If the view received from master cannot guarentee the minimum number
	// of volume partitions, it is probably due to a **temporary** network problem
	// between master and datanode. So do not update the vol group view for
	// now, and use the old information.
	if len(rwPartitionGroups) < MinWritableDataPartitionNum {
		log.LogWarnf("RW Partitions(%v) Minimum(%v)", len(rwPartitionGroups), MinWritableDataPartitionNum)
		return
	}
	w.rwPartition = rwPartitionGroups

	for _, dp := range partitions {
		w.replaceOrInsertPartition(dp)
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

func (w *Wrapper) GetWriteDataPartition(exclude []uint32) (*DataPartition, error) {
	rwPartitionGroups := w.rwPartition
	if len(rwPartitionGroups) == 0 {
		return nil, fmt.Errorf("no writable data partition")
	}

	rand.Seed(time.Now().UnixNano())
	choose := rand.Intn(len(rwPartitionGroups))
	partition := rwPartitionGroups[choose]
	if !isExcluded(partition.PartitionID, exclude) {
		return partition, nil
	}

	for _, partition = range rwPartitionGroups {
		if !isExcluded(partition.PartitionID, exclude) {
			return partition, nil
		}
	}
	return nil, fmt.Errorf("no writable data partition")
}

func (w *Wrapper) GetDataPartition(partitionID uint32) (*DataPartition, error) {
	w.RLock()
	defer w.RUnlock()
	dp, ok := w.partitions[partitionID]
	if !ok {
		return nil, fmt.Errorf("DataPartition[%v] not exsit", partitionID)
	}
	return dp, nil
}

func (w *Wrapper) GetConnect(addr string) (*net.TCPConn, error) {
	return w.conns.Get(addr)
}

func (w *Wrapper) PutConnect(conn *net.TCPConn, forceClose bool) {
	w.conns.Put(conn, forceClose)
}
