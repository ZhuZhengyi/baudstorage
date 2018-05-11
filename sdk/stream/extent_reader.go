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
	"github.com/tiglabs/baudstorage/util/log"
)

type ExtentReader struct {
	inode            uint64
	startInodeOffset int
	endInodeOffset   int
	cache            *CacheBuffer
	vol              *sdk.VolGroup
	key              ExtentKey
	wraper           *sdk.VolGroupWraper
	exitCh           chan bool
	cacheReferCh     chan bool
	lastReadOffset   int
}

const (
	DefaultReadBufferSize = 10 * util.MB
)

func NewExtentReader(inode uint64,inInodeOffset int, key ExtentKey, wraper *sdk.VolGroupWraper) (reader *ExtentReader, err error) {
	reader = new(ExtentReader)
	reader.vol, err = wraper.GetVol(key.VolId)
	if err != nil {
		return
	}
	reader.inode=inode
	reader.key = key
	reader.cache = NewCacheBuffer()
	reader.startInodeOffset = inInodeOffset
	reader.endInodeOffset = reader.startInodeOffset + int(key.Size)
	reader.wraper = wraper
	reader.exitCh = make(chan bool, 2)
	reader.cacheReferCh = make(chan bool, 10)
	reader.cacheReferCh <- true
	go reader.asyncFillCache()

	return
}

func (reader *ExtentReader) read(data []byte, offset, size int) (err error) {
	//if reader.getCacheStatus() == AvaliBuffer && offset+size <= reader.cache.getBufferEndOffset() {
	//	reader.cache.copyData(data, offset, size)
	//	return
	//}
	p := NewReadPacket(reader.key, offset, size)
	err = reader.readDataFromVol(p, data)
	reader.setCacheToUnavali()
	if err == nil {
		select {
		case reader.cacheReferCh <- true:
			reader.lastReadOffset = offset
		default:
			return
		}

	}

	return
}

func (reader *ExtentReader) readDataFromVol(p *Packet, data []byte) (err error) {
	rand.Seed(time.Now().UnixNano())
	index := rand.Intn(int(reader.vol.Goal))
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
		return 0, errors.Annotatef(err, reader.toString()+"readDataFromHost vol[%v] cannot get"+
			" connect from host[%v] ", reader.key.VolId, host)

	}
	defer func() {
		if err != nil {
			log.LogError(err.Error())
			conn.Close()
		} else {
			reader.wraper.PutConnect(conn)
		}
	}()
	if err = p.WriteToConn(conn); err != nil {
		err = errors.Annotatef(err, reader.toString()+"readDataFromHost write ReadPacket[%v] to  host[%v] error ",
			p.GetUniqLogId(), host)
		return 0, err
	}
	for {
		err = p.ReadFromConn(conn, proto.ReadDeadlineTime)
		if err != nil {
			err = errors.Annotatef(err, reader.toString()+"readDataFromHost recive ReadPacketReply[%v] to  host[%v] error ",
				p.GetUniqLogId(), host)
			return
		}
		if p.Opcode != proto.OpOk {
			err = errors.Annotatef(fmt.Errorf(reader.toString()+"readDataFromHost packet[%v] from host [%v] opcode err[%v]",
				p.GetUniqLogId(), host, string(p.Data[:p.Size])), "ReciveData Err")
			return
		}
		copy(data[acatualReadSize:acatualReadSize+int(p.Size)], p.Data[:p.Size])
		acatualReadSize += int(p.Size)
		if acatualReadSize >= expectReadSize {
			return
		}
	}

	return
}

func (reader *ExtentReader) updateKey(key ExtentKey) (update bool) {
	if !(key.VolId == reader.key.VolId && key.ExtentId == reader.key.ExtentId) {
		return
	}
	if key.Size <= reader.key.Size {
		return
	}
	reader.key = key
	reader.endInodeOffset = reader.startInodeOffset + int(key.Size)

	return true
}

func (reader *ExtentReader) toString() (m string) {
	return fmt.Sprintf("inode[%v] extentKey[%v] ", reader.inode,
		reader.key.Marshal())
}

func (reader *ExtentReader) fillCache() error {
	if reader.cache.getBufferEndOffset() == int(reader.key.Size) {
		return nil
	}
	reader.setCacheToUnavali()
	bufferSize := int(util.Min(uint64(int(reader.key.Size)-reader.lastReadOffset), uint64(DefaultReadBufferSize)))
	bufferOffset := reader.lastReadOffset
	p := NewReadPacket(reader.key, bufferOffset, bufferSize)
	data := make([]byte, bufferSize)
	err := reader.readDataFromVol(p, data)
	if err != nil {
		return err
	}
	reader.cache.UpdateCache(data, bufferOffset, bufferSize)

	return nil
}

func (reader *ExtentReader) asyncFillCache() {
	for {
		select {
		case <-reader.cacheReferCh:
			reader.fillCache()
		case <-reader.exitCh:
			return
		}
	}
}

const (
	UnavaliBuffer = 1
	AvaliBuffer   = 2
)

type CacheBuffer struct {
	cache       []byte
	startOffset int
	endOffset   int
	sync.Mutex
	isFull bool
	status int
}

func NewCacheBuffer() (buffer *CacheBuffer) {
	buffer = new(CacheBuffer)
	buffer.cache = make([]byte, 0)
	return buffer
}

func (buffer *CacheBuffer) UpdateCache(data []byte, offset, size int) {
	buffer.Lock()
	defer buffer.Unlock()
	buffer.cache = data
	buffer.startOffset = offset
	buffer.endOffset = offset + size
	buffer.status = AvaliBuffer

	return
}

func (buffer *CacheBuffer) copyData(dst []byte, offset, size int) {
	buffer.Lock()
	defer buffer.Unlock()
	copy(dst, buffer.cache[offset:offset+size])
}

func (buffer *CacheBuffer) getBufferEndOffset() int {
	buffer.Lock()
	defer buffer.Unlock()
	return buffer.endOffset
}

func (reader *ExtentReader) setCacheToUnavali() {
	reader.cache.Lock()
	defer reader.cache.Unlock()
	reader.cache.status = UnavaliBuffer
}

func (reader *ExtentReader) setCacheToAvali() {
	reader.cache.Lock()
	defer reader.cache.Unlock()
	reader.cache.status = AvaliBuffer
}

func (reader *ExtentReader) getCacheStatus() int {
	reader.cache.Lock()
	defer reader.cache.Unlock()
	return reader.cache.status
}
