package stream

import (
	"fmt"
	"github.com/juju/errors"
	"github.com/tiglabs/baudstorage/proto"
	"github.com/tiglabs/baudstorage/sdk"
	"github.com/tiglabs/raft/util"
	"math/rand"
	"sync"
	"time"
)

type ExtentReader struct {
	inode            uint64
	startInodeOffset int
	endInodeOffset   int
	buffer           *CacheBuffer
	vol              *sdk.VolGroup
	key              ExtentKey
	wraper           *sdk.VolGroupWraper
	exitCh           chan bool
	updateCh         chan bool
	appReadTime      int64
	lastReadOffset   int
}

const (
	DefaultReadBufferSize = 10 * util.MB
)

func NewExtentReader(inInodeOffset int, key ExtentKey, wraper *sdk.VolGroupWraper) (reader *ExtentReader, err error) {
	reader = new(ExtentReader)
	reader.vol, err = wraper.GetVol(key.VolId)
	if err != nil {
		return
	}
	reader.key = key
	reader.buffer = NewCacheBuffer(0, int(util.Min(uint64(DefaultReadBufferSize), uint64(key.Size))))
	reader.startInodeOffset = inInodeOffset
	reader.endInodeOffset = reader.startInodeOffset + int(key.Size)
	reader.wraper = wraper
	reader.exitCh = make(chan bool, 2)
	reader.updateCh = make(chan bool, 10)
	go reader.asyncFillCache()

	return
}

func (reader *ExtentReader) read(data []byte, offset, size int) (err error) {
	if reader.getCacheStatus() == AvaliBuffer && offset+size <= reader.buffer.getBufferEndOffset() {
		reader.appReadTime = time.Now().Unix()
		reader.buffer.copyData(data, offset, size)
		return
	}
	p := NewReadPacket(reader.key, offset, size)
	data, err = reader.readDataFromVol(p)
	reader.setCacheToUnavali()
	if err == nil {
		select {
		case reader.updateCh <- true:
			reader.lastReadOffset = offset
		default:
			return
		}

	}

	return
}

func (reader *ExtentReader) readDataFromVol(p *Packet) (data []byte, err error) {
	rand.Seed(time.Now().UnixNano())
	index := rand.Intn(int(reader.vol.Goal))
	data = make([]byte, p.Size)
	host := reader.vol.Hosts[index]
	if _, err = reader.readDataFromHost(p, host, data); err != nil {
		goto FORLOOP
	}
	return

FORLOOP:
	for _, host := range reader.vol.Hosts {
		_, err = reader.readDataFromHost(p, host, data)
		if err == nil {
			return
		}
	}

	return
}

func (reader *ExtentReader) readDataFromHost(p *Packet, host string, data []byte) (acatualReadSize int, err error) {
	expectReadSize := int(p.Size)
	conn, err := reader.wraper.GetConnect(host)
	if err != nil {
		return 0, errors.Annotatef(fmt.Errorf(reader.toString()+" vol[%v] not found", reader.key.VolId),
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
		return 0, err
	}
	for {
		err = p.ReadFromConn(conn, proto.ReadDeadlineTime)
		if err != nil {
			err = errors.Annotatef(fmt.Errorf(reader.toString()+" recive dataCache from host[%v] err[%v]", host, err.Error()),
				"ReciveData Err")
			return
		}
		if p.Opcode != proto.OpOk {
			err = errors.Annotatef(fmt.Errorf(reader.toString()+" packet[%v] from host [%v] opcode err[%v]",
				p.GetUniqLogId(), host, string(p.Data[:p.Size])), "ReciveData Err")
			return
		}
		acatualReadSize += int(p.Size)
		copy(data[acatualReadSize:acatualReadSize+int(p.Size)], p.Data[:p.Size])
		if acatualReadSize >= expectReadSize {
			return
		}
	}

	return
}

func (reader *ExtentReader) updateKey(key ExtentKey) {
	if !(key.VolId == reader.key.VolId && key.ExtentId == reader.key.ExtentId && key.Size > reader.key.Size) {
		return
	}
	reader.key = key
	reader.endInodeOffset = reader.startInodeOffset + int(key.Size)
}

func (reader *ExtentReader) toString() (m string) {
	return fmt.Sprintf("inode[%v] extentKey[%v] ", reader.inode,
		reader.key.Marshal())
}

func (reader *ExtentReader) fillCache() error {
	if reader.buffer.getBufferEndOffset() == int(reader.key.Size) {
		return nil
	}
	reader.setCacheToUnavali()
	bufferSize := int(util.Min(uint64(int(reader.key.Size)-reader.lastReadOffset), uint64(DefaultReadBufferSize)))
	bufferOffset := reader.lastReadOffset
	p := NewReadPacket(reader.key, bufferOffset, bufferSize)
	data, err := reader.readDataFromVol(p)
	if err != nil {
		return err
	}
	reader.buffer.UpdateData(data, bufferOffset, bufferSize)

	return nil
}

func (reader *ExtentReader) asyncFillCache() {
	for {
		select {
		case <-reader.updateCh:
			reader.fillCache()
		}
	}
}

const (
	UnavaliBuffer = 1
	AvaliBuffer   = 2
)

type CacheBuffer struct {
	data        []byte
	startOffset int
	endOffset   int
	sync.Mutex
	isFull bool
	status int
}

func NewCacheBuffer(offset, size int) (buffer *CacheBuffer) {
	buffer = new(CacheBuffer)
	buffer.data = make([]byte, 0)
	buffer.endOffset = offset + size
	buffer.startOffset = offset
	return buffer
}

func (buffer *CacheBuffer) UpdateData(data []byte, offset, size int) {
	buffer.Lock()
	defer buffer.Unlock()
	buffer.data = data
	buffer.startOffset = offset
	buffer.endOffset = offset + size

	return
}

func (buffer *CacheBuffer) isFullCache() bool {
	buffer.Lock()
	defer buffer.Unlock()
	return buffer.isFull
}

func (buffer *CacheBuffer) copyData(dst []byte, offset, size int) {
	buffer.Lock()
	defer buffer.Unlock()
	copy(dst, buffer.data[offset:offset+size])
}

func (buffer *CacheBuffer) getBufferEndOffset() int {
	buffer.Lock()
	defer buffer.Unlock()
	return buffer.endOffset
}

func (reader *ExtentReader) setCacheToUnavali() {
	reader.buffer.Lock()
	defer reader.buffer.Unlock()
	reader.buffer.status = UnavaliBuffer
}

func (reader *ExtentReader) setCacheToAvali() {
	reader.buffer.Lock()
	defer reader.buffer.Unlock()
	reader.buffer.status = AvaliBuffer
}

func (reader *ExtentReader) getCacheStatus() int {
	reader.buffer.Lock()
	defer reader.buffer.Unlock()
	return reader.buffer.status
}
