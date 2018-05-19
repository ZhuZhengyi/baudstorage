package stream

import (
	"encoding/json"
	"fmt"
	"github.com/tiglabs/baudstorage/proto"
	"github.com/tiglabs/baudstorage/util/log"
	"math/rand"
	"testing"
	"time"
)

var (
	sk *StreamKey
)

func updateKey123(inode uint64) (extents []proto.ExtentKey, err error) {
	return sk.Extents, nil
}

type ReaderInfo struct {
	Extent string
	Offset int
	Size   int
}

func TestStreamReader_GetReader(t *testing.T) {
	log.NewLog("log", "test", log.DebugLevel)
	sk = NewStreamKey(2)
	for i := 0; i < 10000; i++ {
		rand.Seed(time.Now().UnixNano())
		ek := proto.ExtentKey{VolId: uint32(rand.Intn(1000)), ExtentId: rand.Uint64() % CFSEXTENTSIZE, Size: uint32(rand.Intn(CFSEXTENTSIZE))}
		sk.Put(ek)

	}
	reader, _ := NewStreamReader(2, nil, updateKey123)
	sumSize := sk.Size()
	haveReadSize := 0
	for {
		if sumSize <= 0 {
			break
		}
		rand.Seed(time.Now().UnixNano())
		currReadSize := rand.Intn(CFSEXTENTSIZE)
		if haveReadSize+currReadSize > int(sk.Size()) {
			currReadSize = int(sk.Size()) - haveReadSize
		}
		canRead, err := reader.initCheck(haveReadSize, currReadSize)
		if err != nil {
			log := fmt.Sprintf("Offset[%v] Size[%v] fileSize[%v] canRead[%v] err[%v]", haveReadSize, currReadSize, sk.Size(), canRead, err)
			t.Log(log)
			t.FailNow()
		}
		extents, extentsOffset, extentsSizes := reader.GetReader(haveReadSize, currReadSize)
		readerInfos := make([]*ReaderInfo, 0)
		for index, e := range extents {
			ri := &ReaderInfo{Extent: e.toString(), Offset: extentsOffset[index], Size: extentsSizes[index]}
			readerInfos = append(readerInfos, ri)
		}
		mesg, _ := json.Marshal(readerInfos)
		fmt.Printf("offset:[%v] size[%v] readers[%v]", haveReadSize, currReadSize, string(mesg))
		haveReadSize += currReadSize
		rand.Seed(time.Now().UnixNano())
		ek := proto.ExtentKey{VolId: uint32(rand.Intn(1000)), ExtentId: rand.Uint64() % CFSEXTENTSIZE, Size: uint32(rand.Intn(CFSEXTENTSIZE) + haveReadSize)}
		sk.Put(ek)
	}
}
