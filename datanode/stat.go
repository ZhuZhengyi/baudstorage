package datanode

import (
	"bytes"
	"fmt"
	"github.com/juju/errors"
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

	Zone                      string
	CurrentConns              int64
	ClusterID                 string
	TcpAddr                   string
	Start                     time.Time
	Total                     uint64
	Used                      uint64
	Free                      uint64
	CreatedVolWeights         uint64 //volCnt*volsize
	RemainWeightsForCreateVol uint64 //all-usedvolsWieghts
	CreatedVolCnt             uint64
	MaxWeightsForCreateVol    uint64

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

func (s *Stats) updateMetrics(total, used, free, createdVolWeights, remainWeightsForCreateVol, maxWeightsForCreateVol, volcnt uint64) {
	s.Lock()
	defer s.Unlock()
	s.Total = total
	s.Used = used
	s.Free = free
	s.CreatedVolWeights = createdVolWeights
	s.RemainWeightsForCreateVol = remainWeightsForCreateVol
	s.MaxWeightsForCreateVol = maxWeightsForCreateVol
	s.CreatedVolCnt = volcnt
}

func post(data []byte, url string) (*http.Response, error) {
	client := &http.Client{}
	buff := bytes.NewBuffer(data)
	client.Timeout = time.Second
	req, err := http.NewRequest("POST", url, buff)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Connection", "close")

	return client.Do(req)
}

func (s *DataNode) postToMaster(data []byte, url string) (msg []byte, err error) {
	success := false
	var err1 error
	for i := 0; i < len(s.masterAddrs); i++ {
		var resp *http.Response
		if masterAddr == "" {
			index := atomic.AddUint32(&s.masterAddrIndex, 1)
			if index >= uint32(len(s.masterAddrs)) {
				index = 0
			}
			masterAddr = s.masterAddrs[index]
		}
		err = nil
		resp, err = post(data, "http://"+masterAddr+url)
		if err != nil {
			index := atomic.AddUint32(&s.masterAddrIndex, 1)
			if index >= uint32(len(s.masterAddrs)) {
				index = 0
			}
			masterAddr = s.masterAddrs[index]
			err = errors.Annotatef(err, ActionPostToMaster+" url[%v] Index[%v]", url, i)
			continue
		}
		scode := resp.StatusCode
		msg, _ = ioutil.ReadAll(resp.Body)
		resp.Body.Close()
		if scode == http.StatusForbidden {
			masterAddr = string(msg)
			masterAddr = strings.Replace(masterAddr, "\n", "", -1)
			log.LogWarn(fmt.Sprintf("%v master Addr change to %v, retry post to master", ActionPostToMaster, string(msg)))
			continue
		}
		if scode != http.StatusOK {
			return nil, fmt.Errorf("postTo %v scode %v msg %v", url, scode, string(msg))
		}
		success = true
		break
	}
	if !success {
		return nil, fmt.Errorf("postToMaster err[%v]", err)
	}

	return msg, err1
}
