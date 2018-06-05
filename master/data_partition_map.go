package master

import (
	"encoding/json"
	"fmt"
	"github.com/tiglabs/baudstorage/util/log"
	"runtime"
	"sync"
	"time"
)

type DataPartitionMap struct {
	sync.RWMutex
	dataPartitionMap           map[uint64]*DataPartition
	dataPartitionCount         int
	readWriteDataPartitions    int
	lastCheckPartitionID       uint64
	lastReleasePartitionID     uint64
	dataPartitions             []*DataPartition
	cacheDataPartitionResponse []byte
}

func NewDataPartitionMap() (dpMap *DataPartitionMap) {
	dpMap = new(DataPartitionMap)
	dpMap.dataPartitionMap = make(map[uint64]*DataPartition, 0)
	dpMap.dataPartitionCount = 1
	dpMap.dataPartitions = make([]*DataPartition, 0)

	return
}

func (dpMap *DataPartitionMap) getDataPartition(ID uint64) (*DataPartition, error) {
	dpMap.RLock()
	defer dpMap.RUnlock()
	if v, ok := dpMap.dataPartitionMap[ID]; ok {
		return v, nil
	}
	log.LogError(fmt.Sprintf("action[getDataPartition],partitionID:%v,err:%v", ID, DataPartitionNotFound))
	return nil, DataPartitionNotFound
}

func (dpMap *DataPartitionMap) putDataPartition(dp *DataPartition) {
	dpMap.Lock()
	defer dpMap.Unlock()
	dpMap.dataPartitionMap[dp.PartitionID] = dp
	dpMap.dataPartitions = append(dpMap.dataPartitions, dp)
}

func (dpMap *DataPartitionMap) updateDataPartitionResponseCache(needUpdate bool, minPartitionID uint64) (body []byte, err error) {
	dpMap.Lock()
	defer dpMap.Unlock()
	if dpMap.cacheDataPartitionResponse == nil || needUpdate || len(dpMap.cacheDataPartitionResponse) == 0 {
		dpMap.cacheDataPartitionResponse = make([]byte, 0)
		dpResps := dpMap.GetDataPartitionsView(minPartitionID)
		if len(dpResps) == 0 {
			log.LogError(fmt.Sprintf("action[updateDataPartitionResponseCache],minPartitionID:%v,err:%v",
				minPartitionID, NoAvailDataPartition))
			return nil, NoAvailDataPartition
		}
		cv := NewDataPartitionsView()
		cv.DataPartitions = dpResps
		if body, err = json.Marshal(cv); err != nil {
			log.LogError(fmt.Sprintf("action[updateDataPartitionResponseCache],minPartitionID:%v,err:%v",
				minPartitionID, err.Error()))
			return nil, fmt.Errorf("%v,marshal err:%v", NoAvailDataPartition, err.Error())
		}
		dpMap.cacheDataPartitionResponse = body
		return
	}
	body = make([]byte, len(dpMap.cacheDataPartitionResponse))
	copy(body, dpMap.cacheDataPartitionResponse)

	return
}

func (dpMap *DataPartitionMap) GetDataPartitionsView(minPartitionID uint64) (dpResps []*DataPartitionResponse) {
	dpResps = make([]*DataPartitionResponse, 0)
	log.LogDebugf("DataPartitionMapLen[%v],DataPartitionsLen[%v],minPartitionID[%v],dataPartitionMap[%v],dataPartitions[%v]", len(dpMap.dataPartitionMap), len(dpMap.dataPartitions), minPartitionID, dpMap.dataPartitionMap, dpMap.dataPartitions)
	for _, dp := range dpMap.dataPartitionMap {
		if dp.PartitionID <= minPartitionID {
			continue
		}
		dpResp := dp.convertToDataPartitionResponse()
		dpResps = append(dpResps, dpResp)
	}

	return
}

func (dpMap *DataPartitionMap) getNeedReleaseDataPartitions(everyReleaseDataPartitionCount int, releaseDataPartitionAfterLoadSeconds int64) (partitions []*DataPartition) {
	partitions = make([]*DataPartition, 0)
	dpMap.RLock()
	defer dpMap.RUnlock()

	for i := 0; i < everyReleaseDataPartitionCount; i++ {
		if dpMap.lastReleasePartitionID > (uint64)(len(dpMap.dataPartitionMap)) {
			dpMap.lastReleasePartitionID = 0
		}
		dpMap.lastReleasePartitionID++
		dp, ok := dpMap.dataPartitionMap[dpMap.lastReleasePartitionID]
		if ok && time.Now().Unix()-dp.LastLoadTime >= releaseDataPartitionAfterLoadSeconds {
			partitions = append(partitions, dp)
		}
	}

	return
}

func (dpMap *DataPartitionMap) releaseDataPartitions(partitions []*DataPartition) {
	defer func() {
		if err := recover(); err != nil {
			const size = RuntimeStackBufSize
			buf := make([]byte, size)
			buf = buf[:runtime.Stack(buf, false)]
			log.LogError(fmt.Sprintf("releaseDataPartitions panic %v: %s\n", err, buf))
		}
	}()
	var wg sync.WaitGroup
	for _, dp := range partitions {
		wg.Add(1)
		go func(dp *DataPartition) {
			dp.ReleaseDataPartition()
			wg.Done()
		}(dp)
	}
	wg.Wait()

}

func (dpMap *DataPartitionMap) getNeedCheckDataPartitions(everyLoadCount int, loadFrequencyTime int64) (partitions []*DataPartition) {
	partitions = make([]*DataPartition, 0)
	dpMap.RLock()
	defer dpMap.RUnlock()

	for i := 0; i < everyLoadCount; i++ {
		if dpMap.lastCheckPartitionID > (uint64)(len(dpMap.dataPartitionMap)) {
			dpMap.lastCheckPartitionID = 0
		}
		dpMap.lastCheckPartitionID++
		v, ok := dpMap.dataPartitionMap[dpMap.lastCheckPartitionID]
		if ok && time.Now().Unix()-v.LastLoadTime >= loadFrequencyTime {
			partitions = append(partitions, v)
		}
	}

	return
}
