package stream

import (
	"github.com/tiglabs/baudstorage/proto"
	"github.com/tiglabs/baudstorage/sdk"
	"math/rand"
	"net"
	"sync"
	"time"
)

type ExtentReader struct {
	startInodeOffset int
	endInodeOffset   int
	data             []byte
	size             int
	key              ExtentKey
	wraper           *sdk.VolGroupWraper
	byteRecive       int
	sync.Mutex
}

func NewExtentReader(inInodeOffset int, key ExtentKey, wraper *sdk.VolGroupWraper) (reader *ExtentReader) {
	reader = new(ExtentReader)
	reader.data = make([]byte, 0)
	reader.key = key
	reader.startInodeOffset = inInodeOffset
	reader.size = int(key.Size)
	reader.endInodeOffset = reader.startInodeOffset + reader.size
	reader.wraper = wraper

	return reader
}

func (reader *ExtentReader) streamRecivePacket(p *Packet, conn net.Conn){
	reader.Lock()
	reader.data = make([]byte, 0)
	reader.Unlock()
	var err error
	for {
		err = p.ReadFromConn(conn, proto.ReadDeadlineTime)
		if err != nil {
			break
		}
		if p.Opcode != proto.OpOk {
			break
		}
		reader.Lock()
		reader.byteRecive+=int(p.Size)
		reader.data = append(reader.data, p.Data...)
		if len(reader.data) == reader.size {
			reader.Unlock()
			break
		}
		reader.Unlock()
	}
	if err!=nil {
		conn.Close()
	}
}

func (reader *ExtentReader) reciveData(vol *sdk.VolGroup) {
	rand.Seed(time.Now().UnixNano())
	index := rand.Intn(int(vol.Goal))
	p := NewReadPacket(vol, reader.key)
	host := vol.Hosts[index]
	conn, err := reader.wraper.GetConnect(host)
	if err != nil {
		goto FORLOOP
	}
	if err = p.WriteToConn(conn); err != nil {
		goto FORLOOP
	}
	go reader.streamRecivePacket(p, conn)

FORLOOP:
	for _, host := range vol.Hosts {
		conn, err := reader.wraper.GetConnect(host)
		if err != nil {
			continue
		}
		if err = p.WriteToConn(conn); err != nil {
			continue
		}
		go reader.streamRecivePacket(p, conn)
		break

	}
}
