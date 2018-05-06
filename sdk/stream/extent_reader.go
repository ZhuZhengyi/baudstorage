package stream

import (
	"github.com/tiglabs/baudstorage/sdk"
	"sync"
	"math/rand"
	"time"
)

type ExtentReader struct {
	startInodeOffset int
	endInodeOffset   int
	data             []byte
	size             int
	key              ExtentKey
	wraper           *sdk.VolGroupWraper
	sync.Mutex
}


type Buffer struct {
	r *ExtentReader

}

func NewExtentReader(inInodeOffset int, key ExtentKey,wraper *sdk.VolGroupWraper) (reader *ExtentReader) {
	reader = new(ExtentReader)
	reader.data = make([]byte, 0)
	reader.key = key
	reader.startInodeOffset = inInodeOffset
	reader.size = int(key.Size)
	reader.endInodeOffset =reader.startInodeOffset +reader.size
	reader.wraper=wraper

	return reader
}


func (reader *ExtentReader)referData(){
	vol,err:=reader.wraper.GetVol(reader.key.VolId)
	if err!=nil {
		return
	}
	rand.Seed(time.Now().UnixNano())
	index:=rand.Intn(int(vol.Goal))
	p:=NewReadPacket(vol,reader.key)
	host:=vol.Hosts[index]
	p.WriteToConn()
}