package stream

import (
	"fmt"
	"github.com/juju/errors"
	"github.com/tiglabs/baudstorage/proto"
	"github.com/tiglabs/baudstorage/sdk"
	"github.com/tiglabs/raft/logger"
	"math/rand"
	"sync"
	"time"
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
	exitCh   chan bool
	updateCh chan bool
}

func NewExtentReader(inInodeOffset int, key ExtentKey, wraper *sdk.VolGroupWraper) (reader *ExtentReader) {
	reader = new(ExtentReader)
	reader.data = make([]byte, 0)
	reader.key = key
	reader.startInodeOffset = inInodeOffset
	reader.size = int(key.Size)
	reader.endInodeOffset = reader.startInodeOffset + reader.size
	reader.wraper = wraper
	reader.exitCh = make(chan bool, 2)
	reader.updateCh = make(chan bool, 10)
	go reader.asyncRecivData()

	return reader
}

func (reader *ExtentReader) sendAndReciveExtentData(host string) error {
	reader.Lock()
	p := NewReadPacket(reader.key, reader.byteRecive)
	reader.Unlock()
	conn, err := reader.wraper.GetConnect(host)
	if err != nil {
		return errors.Annotatef(fmt.Errorf(reader.toString()+" vol[%v] not found", reader.key.VolId),
			"ReciveData Err")

	}
	defer func() {
		if err != nil {
			conn.Close()
		} else {
			reader.wraper.PutConnect(conn)
		}
	}()
	if err = p.WriteToConn(conn); err != nil {
		err = errors.Annotatef(fmt.Errorf(reader.toString()+" cannot get connect from host[%v] err[%v]", host, err.Error()),
			"ReciveData Err")
		return err
	}
	for {
		err = p.ReadFromConn(conn, proto.ReadDeadlineTime)
		if err != nil {
			err = errors.Annotatef(fmt.Errorf(reader.toString()+" recive data from host[%v] err[%v]", host, err.Error()),
				"ReciveData Err")
			return err
		}
		if p.Opcode != proto.OpOk {
			err = errors.Annotatef(fmt.Errorf(reader.toString()+" packet[%v] from host [%v] opcode err[%v]",
				p.GetUniqLogId(), host, string(p.Data[:p.Size])), "ReciveData Err")
			return err
		}
		reader.Lock()
		reader.data = append(reader.data, p.Data...)
		reader.byteRecive += int(p.Size)
		if len(reader.data) == reader.size {
			reader.Unlock()
			break
		}
		reader.Unlock()
	}

	return err
}

func (reader *ExtentReader) toString() (m string) {
	return fmt.Sprintf("inode[%v] extentKey[%v] bytesRecive[%v] ", reader.inode,
		reader.key.Marshal(), reader.byteRecive)
}

func (reader *ExtentReader) reciveData() error {
	reader.Lock()
	if reader.byteRecive == reader.size {
		reader.Unlock()
		return nil
	}
	reader.Unlock()
	rand.Seed(time.Now().UnixNano())
	vol, err := reader.wraper.GetVol(reader.key.VolId)
	if err != nil {
		err = errors.Annotatef(fmt.Errorf(reader.toString()+" vol[%v] not found", reader.key.VolId),
			"ReciveData Err")
		logger.Error(err.Error())
		return err
	}
	index := rand.Intn(int(vol.Goal))
	host := vol.Hosts[index]
	if err = reader.sendAndReciveExtentData(host); err != nil {
		goto FORLOOP
	}
	return nil

FORLOOP:
	for _, host := range vol.Hosts {
		err = reader.sendAndReciveExtentData(host)
		if err == nil {
			return nil
		}
	}

	return err
}

func (reader *ExtentReader) asyncRecivData() {
	err := reader.reciveData()
	if err != nil {
		reader.updateCh <- true
	}
	for {
		select {
		case <-reader.exitCh:
			return
		case <-reader.updateCh:
			err := reader.reciveData()
			if err != nil {
				reader.updateCh <- true
			}
		}
	}
}

func (reader *ExtentReader) read(data []byte, offset, size int) {
	reader.Lock()
	if offset+size <= reader.byteRecive {
		reader.Unlock()
		copy(data, reader.data[offset:offset+size])
		return
	}
	reader.Unlock()

	return
}
