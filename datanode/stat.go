package datanode

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/tiglabs/baudstorage/storage"
	"github.com/tiglabs/baudstorage/util/log"
	"io/ioutil"
	"net/http"
	"strings"
	"sync/atomic"
	"time"
)

var (
	masterAddr string
)

type VolStat struct {
	ID     uint32 `json:"VolID"`
	Status int32  `json:"VolStat"`

	//report to monitor
	ReadLatency  int32 `json:"ReadDelay"`
	WriteLatency int32 `json:"WriteDelay"`
	ReadBytes    int64
	WriteBytes   int64
	Files        int
	Total        int
	Used         int64
}

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
	VolInfo         []*VolStat
}

//various metrics such free and total storage space, traffic, etc
type Stats struct {
	role        uint8
	inDataSize  uint64
	outDataSize uint64
	inFlow      uint64
	outFlow     uint64

	Zone               string
	CurrentConns       int64
	MaxDiskAvailWeight int64
	TotalWeight        uint64
	UsedWeight         uint64
	Ver                string
	ClusterID          string
	TcpAddr            string
	Start              time.Time

	VolsInfo []*VolStat `json:"VolInfo"`
	DiskInfo []*DiskMetrics
}

func NewStats(role uint8, ver, cltaddr string, zone string, total uint64) *Stats {
	s := new(Stats)
	s.role = role
	s.Start = time.Now()
	s.Ver = ver
	s.Zone = zone
	s.TcpAddr = cltaddr
	s.TotalWeight = total

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

func (s *Stats) UpdateDataSize(takeTime uint64) {
	inData := atomic.LoadUint64(&s.inDataSize)
	outData := atomic.LoadUint64(&s.outDataSize)
	atomic.StoreUint64(&s.inFlow, inData/takeTime)
	atomic.StoreUint64(&s.outFlow, outData/takeTime)

	atomic.StoreUint64(&s.inDataSize, 0)
	atomic.StoreUint64(&s.outDataSize, 0)
}

func (s *Stats) GetFlow() (uint64, uint64) {
	return atomic.LoadUint64(&s.inFlow), atomic.LoadUint64(&s.outFlow)
}

func (s *Stats) ToJSON() ([]byte, error) {
	return json.Marshal(s)
}

func GetVolReport(d *Disk, space *SpaceManager, flag uint8) []*VolStat {
	volIDs := d.getVols()
	volReport := make([]*VolStat, len(volIDs))

	for i, vID := range volIDs {
		vr := new(VolStat)
		v := space.getVol(vID)
		if v == nil {
			continue
		}

		vr.ID = uint32(vID)
		vr.Status = int32(v.status)
		if flag == ReportToMonitorRole {
			vr.Files, _ = v.Store.GetStoreFileCount()
			vr.ReadLatency, vr.ReadBytes = v.GetReadRecord()
			vr.WriteLatency, vr.WriteBytes = v.GetWriteRecord()
		}
		switch v.volMode {
		case TinyVol:
			store := v.store.(*storage.TinyStore)
			vr.Total = v.volSize
			vr.Used = store.GetStoreUsedSize()
		case ExtentVol:
			store := v.store.(*storage.ExtentStore)
			vr.Total = v.volSize
			vr.Used = store.GetStoreUsedSize()
		}
		volReport[i] = vr
	}

	return volReport
}

func GetDiskReport(d *Disk, space *SpaceManager, flag uint8) (metrics *DiskMetrics) {
	metrics = new(DiskMetrics)
	metrics.Status = int32(d.Status)
	metrics.ReadErrs = int32(d.ReadErrs)
	metrics.WriteErrs = int32(d.WriteErrs)
	metrics.MaxDiskErrs = int32(d.MaxErrs)
	metrics.MinRestWeight = int64(d.RestSize)
	metrics.TotalWeight = int64(d.All)
	metrics.RealAvailWeight = int64(d.Free)
	metrics.VolAvailWeight = int64(d.VolFree)
	metrics.VolInfo = GetVolReport(d, space, flag)

	return
}

func (s *Stats) GetStat(space *SpaceManager) ([]byte, error) {
	if s.role == ReportToSelfRole {
		return json.MarshalIndent(*s, "", "\t")
	}

	i, totalAvailWeight := 0, uint64(0)
	s.MaxDiskAvailWeight = 0
	if s.role == ReportToMonitorRole {
		s.DiskInfo = make([]*DiskMetrics, len(space.disks))
		s.VolsInfo = nil
	} else {
		s.VolsInfo = make([]*VolStat, 0)
		s.DiskInfo = nil
	}

	for path, d := range space.disks {
		d.UpdateSpaceInfo(s.TcpAddr)
		if s.role == ReportToMonitorRole {
			s.DiskInfo[i] = GetDiskReport(d, space, s.role)
			s.DiskInfo[i].Path = path
			i++
		} else {
			volReports := GetVolReport(d, space, s.role)
			for _, volrpt := range volReports {
				if volrpt == nil {
					continue
				}
				s.VolsInfo = append(s.VolsInfo, volrpt)
			}
		}
		totalAvailWeight += uint64(d.VolFree)
		if s.MaxDiskAvailWeight < int64(d.VolFree) && d.Status == storage.ReadWriteStore {
			s.MaxDiskAvailWeight = int64(d.VolFree)
		}
	}
	if s.TotalWeight < totalAvailWeight {
		s.UsedWeight = 0
	} else {
		s.UsedWeight = s.TotalWeight - totalAvailWeight
	}
	s.TotalWeight = totalAvailWeight

	return s.ToJSON()
}

func post(data []byte, url string) (*http.Response, error) {
	client := &http.Client{}
	buff := bytes.NewBuffer(data)
	req, err := http.NewRequest("POST", url, buff)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Connection", "close")

	return client.Do(req)
}

func PostToMaster(data []byte, url string) (msg []byte, err error) {
	retry := 0
Retry:
	resp, err := post(data, "http://"+masterAddr+url)
	if err != nil {
		err = fmt.Errorf("postTo %v error %v", url, err.Error())
		return
	}
	scode := resp.StatusCode
	msg, _ = ioutil.ReadAll(resp.Body)
	resp.Body.Close()
	if scode == http.StatusForbidden && retry < 3 {
		masterAddr = string(msg)
		masterAddr = strings.Replace(masterAddr, "\n", "", -1)
		log.LogWarn(fmt.Sprintf("%v master Addr change to %v, retry post to master", ActionPostToMaster, string(msg)))
		retry++
		goto Retry
	}
	if retry >= 3 {
		err = fmt.Errorf("postTo %v scode %v msg %v", url, scode, string(msg))
		return
	}
	if scode != http.StatusOK {
		err = fmt.Errorf("postTo %v scode %v msg %v", url, scode, string(msg))
	}
	return
}
