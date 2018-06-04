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
	DataPartionViewUrl       = "/client/datapartion?name="
	ActionGetDataPartionView = "ActionGetDataPartionView"
)

const (
	MinWritableDataPartionNum = 20
)

type DataPartion struct {
	DataPartionID uint32
	Status        uint8
	ReplicaNum    uint8
	Hosts         []string
}

type DataPartionView struct {
	Partions []*DataPartion
}

func (dp *DataPartion) GetAllAddrs() (m string) {
	return strings.Join(dp.Hosts[1:], proto.AddrSplit) + proto.AddrSplit
}

type DataPartionWrapper struct {
	sync.RWMutex
	namespace string
	master    []string
	conns     *pool.ConnPool
	partions  map[uint32]*DataPartion
	rwPartion []*DataPartion
}

func NewDataPartionWrapper(namespace, masterHosts string) (wrapper *DataPartionWrapper, err error) {
	master := strings.Split(masterHosts, ",")
	wrapper = new(DataPartionWrapper)
	wrapper.master = master
	wrapper.namespace = namespace
	wrapper.conns = pool.NewConnPool()
	wrapper.rwPartion = make([]*DataPartion, 0)
	wrapper.partions = make(map[uint32]*DataPartion)
	if err = wrapper.getDataPartionFromMaster(); err != nil {
		return
	}
	go wrapper.update()
	return
}

func (wrapper *DataPartionWrapper) update() {
	ticker := time.NewTicker(time.Minute * 5)
	for {
		select {
		case <-ticker.C:
			wrapper.getDataPartionFromMaster()
		}
	}
}

func (wrapper *DataPartionWrapper) getDataPartionFromMaster() (err error) {
	for _, m := range wrapper.master {
		if m == "" {
			continue
		}
		var resp *http.Response
		resp, err = http.Get("http://" + m + DataPartionViewUrl + wrapper.namespace)
		if err != nil {
			log.LogError(fmt.Sprintf(ActionGetDataPartionView+"get VolView from master[%v] err[%v]", m, err.Error()))
			continue
		}
		defer resp.Body.Close()
		body, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			continue
		}
		views := new(DataPartionView)
		if err = json.Unmarshal(body, views); err != nil {
			log.LogError(fmt.Sprintf(ActionGetDataPartionView+"get VolView from master[%v] err[%v]", m, err.Error()))
			continue
		}
		log.LogInfof("Get VolView from master: %v", string(body))
		wrapper.updateDataPartion(views.Partions)
		break
	}

	return
}

func (wrapper *DataPartionWrapper) replaceOrInsertPartion(dp *DataPartion) {
	wrapper.Lock()
	defer wrapper.Unlock()
	if _, ok := wrapper.partions[dp.DataPartionID]; ok {
		delete(wrapper.partions, dp.DataPartionID)
	}
	wrapper.partions[dp.DataPartionID] = dp
}

func (wrapper *DataPartionWrapper) updateDataPartion(partions []*DataPartion) {
	rwPartionGroups := make([]*DataPartion, 0)
	for _, dp := range partions {
		if dp.Status == storage.ReadWriteStore {
			rwPartionGroups = append(rwPartionGroups, dp)
		}
	}

	// If the view received from master cannot guarentee the minimum number
	// of volume partions, it is probably due to a **temporary** network problem
	// between master and datanode. So do not update the vol group view for
	// now, and use the old information.
	if len(rwPartionGroups) < MinWritableDataPartionNum {
		return
	}
	wrapper.rwPartion = rwPartionGroups

	for _, dp := range partions {
		wrapper.replaceOrInsertPartion(dp)
	}
}

func isExcluded(partionId uint32, excludes []uint32) bool {
	for _, id := range excludes {
		if id == partionId {
			return true
		}
	}
	return false
}

func (wrapper *DataPartionWrapper) GetWriteDataPartion(exclude []uint32) (*DataPartion, error) {
	rwPartionGroups := wrapper.rwPartion
	if len(rwPartionGroups) == 0 {
		return nil, fmt.Errorf("No writable DataPartion")
	}

	rand.Seed(time.Now().UnixNano())
	choose := rand.Intn(len(rwPartionGroups))
	partion := rwPartionGroups[choose]
	if !isExcluded(partion.DataPartionID, exclude) {
		return partion, nil
	}

	for _, partion = range rwPartionGroups {
		if !isExcluded(partion.DataPartionID, exclude) {
			return partion, nil
		}
	}
	return nil, fmt.Errorf("No writable DataPartion")
}

func (wrapper *DataPartionWrapper) GetDataPartion(partionID uint32) (*DataPartion, error) {
	wrapper.RLock()
	defer wrapper.RUnlock()
	dp, ok := wrapper.partions[partionID]
	if !ok {
		return nil, fmt.Errorf("DataPartion[%v] not exsit", partionID)
	}
	return dp, nil
}

func (wrapper *DataPartionWrapper) GetConnect(addr string) (net.Conn, error) {
	return wrapper.conns.Get(addr)
}

func (wrapper *DataPartionWrapper) PutConnect(conn net.Conn) {
	wrapper.conns.Put(conn)
}
