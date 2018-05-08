package stream

import (
	"github.com/tiglabs/baudstorage/proto"
	"github.com/tiglabs/baudstorage/sdk"
	"math/rand"
	"net"
	"sync"
	"time"
	"github.com/juju/errors"
	"fmt"
	"github.com/tiglabs/raft/logger"
	"google.golang.org/grpc/naming"
)

type ExtentReader struct {
	inode            uint64
	startInodeOffset int
	endInodeOffset   int
	data             []byte
	size             int
	key              ExtentKey
	wraper           *sdk.VolGroupWraper
	byteRecive       int
	sync.Mutex
	isReciving       bool
	exitCh        d     chan bool
	updateCh           chan bool
}

func NewExtentReader(inInodeOffset int, key ExtentKey, wraper *sdk.VolGroupWraper) (reader *ExtentReader) {
	reader = new(ExtentReader)
	reader.data = make([]byte, 0)
	reader.key = key
	reader.startInodeOffset = inInodeOffset
	reader.size = int(key.Size)
	reader.endInodeOffset = reader.startInodeOffset + reader.size
	reader.wraper = wraper
	reader.exitCh=make(chan bool,2)
	reader.updateCh=make(chan bool,1)
	go reader.asyncRecivData()

	return reader
}

func (reader *ExtentReader) streamRecivePacket(host string)(error){
	conn, err := reader.wraper.GetConnect(host)
	if err != nil {
		err=errors.Annotatef(fmt.Errorf(reader.toString()+" vol[%v] not found",reader.key.VolId),
			"ReciveData Err")
		logger.Error(err.Error())


	}
	if err = 7yp.WriteToConn(conn); err != nil {
		err=errors.Annotatef(fmt.Errorf(reader.toString()+" cannot get connect from host[%v] err[%v]",host,err.Error()),
			"ReciveData Err")
		logger.Error(err.Error())
		continue
	}
	err=reader.streamRecivePacket(p, conn)
	if err!=nil {
		continue
	}
	for {
		err = p.ReadFromConn(conn, proto.ReadDeadlineTime)
		if err != nil {
			break
		}
		if p.Opcode != proto.OpOk {
			err=errors.Annotatef(fmt.Errorf(reader.toString()+" packet[%v] opcode err[%v]",
				p.GetUniqLogId(),string(p.Data[:p.Size])),"streamRecivePacket Err")
			break
		}
		reader.Lock()
		reader.data = append(reader.data, p.Data...)
		reader./+=int(p.Size)
		if len(reader.data) == reader.size {
			reader.Unlock()
			break
		}
		reader.Unlock()
	}
	if err!=nil {
		conn.Close()
	}else{
		reader.wraper.PutConnect(conn)
	}
}


func (reader *ExtentReader) toString() (m string) {
	return fmt.Sprintf("inode[%v] extentKey[%v] bytesRecive[%v] ", reader.inode,
		reader.key.Marshal(), reader.byteRecive)
}

func (reader *ExtentReader) reciveData()(err error) {
	var (
		vol *sdk.VolGroup
		p *Packet
		conn net.Conn
	)
	if reader.byteRecive==reader.size{
		return
	}
	rand.Seed(time.Now().UnixNano())
	vol,err=reader.wraper.GetVol(reader.key.VolId)
	if err!=nil {
		err=errors.Annotatef(fmt.Errorf(reader.toString()+" vol[%v] not found",reader.key.VolId),
			"ReciveData Err")
		logger.Error(err.Error())
		return err
	}
	index := rand.Intn(int(vol.Goal))
	p = NewReadPacket(vol, reader.key,reader.byteRecive)
	host := vol.Hosts[index]
	conn, err = reader.wraper.GetConnect(host)
	if err != nil {
		err=errors.Annotatef(fmt.Errorf(reader.toString()+" cannot get connect from host[%v] err[%v]",host,err.Error()),
			"ReciveData Err")
		logger.Error(err.Error())
		goto FORLOOP
	}
	if err = p.WriteToConn(conn); err != nil {
		err=errors.Annotatef(fmt.Errorf(reader.toString()+" write to host[%v] err[%v]",host,err.Error()),
			"ReciveData Err")
		logger.Error(err.Error())
		goto FORLOOP
	}
	err=reader.streamRecivePacket(p, conn)
	if err==nil {
		return
	}

FORLOOP:
	for _, host := range vol.Hosts {


	}
	return
}


func (reader *ExtentReader)asyncRecivData(){
	reader.reciveData()
	for {
		select {
			case <-reader.exitCh:
				return
			case <-reader.updateCh:
				err:=reader.reciveData()
				if err!=nil {
					reader.updateCh<-true
				}
		}
	}
}

func (reader *ExtentReader)read(data []byte,offset,size int){
	reader.Lock()
	if offset+size<reader.byteRecive{
		reader.Unlock()
		copy(data,reader.data[offset:offset+size])
		return
	}
	reader.Unlock()




	return
}