package datanode

import (
	"bytes"
	"fmt"
	"github.com/tiglabs/baudstorage/util/log"
	"io/ioutil"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type DiskMetrics struct {
	Status          int32
	ReadErrs        int32
	WriteErrs       int32
	MaxDiskErrs     int32
	MinRestWeight   int64
	TotalWeight     int64
	RealAvailWeight int64
	VolAvailWeight  int64
	Path            string
}

//various metrics such free and total storage space, traffic, etc
type Stats struct {
	inDataSize  uint64
	outDataSize uint64
	inFlow      uint64
	outFlow     uint64

	Zone                          string
	CurrentConns                  int64
	ClusterID                     string
	TcpAddr                       string
	Start                         time.Time
	Total                         uint64
	Used                          uint64
	Free                          uint64
	CreatedPartionWeights         uint64 //dataPartionCnt*dataPartionsize
	RemainWeightsForCreatePartion uint64 //all-useddataPartionsWieghts
	CreatedPartionCnt             uint64
	MaxWeightsForCreatePartion    uint64

	sync.Mutex
}

func NewStats(zone string) (s *Stats) {
	s = new(Stats)
	s.Zone = zone
	return s
}

func (s *Stats) AddConnection() {
	atomic.AddInt64(&s.CurrentConns, 1)
}

func (s *Stats) RemoveConnection() {
	atomic.AddInt64(&s.CurrentConns, -1)
}

func (s *Stats) GetConnectionNum() int64 {
	return atomic.LoadInt64(&s.CurrentConns)
}

func (s *Stats) AddInDataSize(size uint64) {
	atomic.AddUint64(&s.inDataSize, size)
}

func (s *Stats) AddOutDataSize(size uint64) {
	atomic.AddUint64(&s.outDataSize, size)
}

func (s *Stats) updateMetrics(total, used, free, createdVolWeights, remainWeightsForCreateVol, maxWeightsForCreateVol, dataPartioncnt uint64) {
	s.Lock()
	defer s.Unlock()
	s.Total = total
	s.Used = used
	s.Free = free
	s.CreatedPartionWeights = createdVolWeights
	s.RemainWeightsForCreatePartion = remainWeightsForCreateVol
	s.MaxWeightsForCreatePartion = maxWeightsForCreateVol
	s.CreatedPartionCnt = dataPartioncnt
}

func post(data []byte, url string) (*http.Response, error) {
	client := &http.Client{}
	buff := bytes.NewBuffer(data)
	client.Timeout = time.Second * 3
	req, err := http.NewRequest("POST", url, buff)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Connection", "close")

	return client.Do(req)
}

func PostToMaster(data []byte, url string) (msg []byte, err error) {
	success := false
	var err1 error
	log.LogDebugf("action[DataNode.postToMaster] masterAddrs[%v].", MasterAddrs)

	for i := 0; i < len(MasterAddrs); i++ {
		var resp *http.Response
		if CurrMaster == "" {
			index := atomic.AddUint32(&MasterAddrIndex, 1)
			if index >= uint32(len(MasterAddrs)) {
				index = 0
			}
			CurrMaster = MasterAddrs[index]
		}
		err = nil
		resp, err = post(data, "http://"+CurrMaster+url)
		if err != nil {
			index := atomic.AddUint32(&MasterAddrIndex, 1)
			if index >= uint32(len(MasterAddrs)) {
				index = 0
			}
			CurrMaster = MasterAddrs[index]
			resp, err = post(data, "http://"+CurrMaster+url)
		}
		scode := resp.StatusCode
		msg, _ = ioutil.ReadAll(resp.Body)
		resp.Body.Close()
		if scode == http.StatusForbidden {
			CurrMaster = string(msg)
			CurrMaster = strings.Replace(CurrMaster, "\n", "", 100)
			log.LogWarn(fmt.Sprintf("action[DataNode.postToMaster] master Addr change to %v, retry post to master", string(msg)))
			continue
		}
		if scode != http.StatusOK {
			return nil, fmt.Errorf("postTo %v scode %v msg %v", url, scode, string(msg))
		}
		success = true
		log.LogInfof("action[DataNode.postToMaster] url[%v] to master[%v] response[%v] code[%v]", url, MasterAddrs, string(msg), scode)
		break
	}
	if !success {
		return nil, fmt.Errorf("postToMaster err[%v]", err)
	}

	return msg, err1
}
