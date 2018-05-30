package stream

import (
	"fmt"
	"github.com/juju/errors"
	"github.com/tiglabs/baudstorage/proto"
	"github.com/tiglabs/baudstorage/sdk/vol"
	"github.com/tiglabs/baudstorage/util"
	"github.com/tiglabs/baudstorage/util/log"
	"math/rand"
	"sync"
	"time"
)

type ExtentReader struct {
	inode            uint64
	startInodeOffset int
	endInodeOffset   int
	cache            *CacheBuffer
	vol              *vol.VolGroup
	key              proto.ExtentKey
	wrapper          *vol.VolGroupWrapper
	exitCh           chan bool
	cacheReferCh     chan bool
	lastReadOffset   int
	sync.Mutex
}

const (
	DefaultReadBufferSize = 10 * util.MB
)

func NewExtentReader(inode uint64, inInodeOffset int, key proto.ExtentKey,
	wrapper *vol.VolGroupWrapper) (reader *ExtentReader, err error) {
	reader = new(ExtentReader)
	reader.vol, err = wrapper.GetVol(key.VolId)
	if err != nil {
		return
	}
	reader.inode = inode
	reader.key = key
	reader.cache = NewCacheBuffer()
	reader.startInodeOffset = inInodeOffset
	reader.endInodeOffset = reader.startInodeOffset + int(key.Size)
	reader.wrapper = wrapper
	reader.exitCh = make(chan bool, 2)
	//reader.cacheReferCh = make(chan bool, 10)
	//reader.cacheReferCh <- true
	//go reader.asyncFillCache()

	return
}

func (reader *ExtentReader) read(data []byte, offset, size int) (err error) {
	//if reader.getCacheStatus() == AvaliBuffer && Offset+Size <= reader.cache.getBufferEndOffset() {
	//	reader.cache.copyData(data, Offset, Size)
	//	return
	//}

	err = reader.readDataFromVol(data,offset,size)
	//reader.setCacheToUnavali()
	//if err == nil {
	//	select {
	//	case reader.cacheReferCh <- true:
	//		reader.lastReadOffset = Offset
	//	default:
	//		return
	//	}
	//
	//}

	return
}

func (reader *ExtentReader) readDataFromVol(data []byte,offset,size int) (err error) {
	rand.Seed(time.Now().UnixNano())
	index := rand.Intn(int(reader.vol.ReplicaNum))
	host := reader.vol.Hosts[index]
	if _, err = reader.readDataFromHost(host, data,offset,size); err != nil {
		log.LogError(err.Error())
		goto FORLOOP
	}
	return

FORLOOP:
	for _, host := range reader.vol.Hosts {
		_, err = reader.readDataFromHost(host, data,offset,size)
		if err == nil {
			return
		} else {
			log.LogError(err.Error())
		}
	}

	return
}

func (reader *ExtentReader) readDataFromHost( host string, data []byte,offset,size int) (acatualReadSize int, err error) {
	reader.Lock()
	p:=NewReadPacket(reader.key,offset,size)
	reader.Unlock()
	expectReadSize := int(p.Size)
	conn, err := reader.wrapper.GetConnect(host)
	if err != nil {
		return 0, errors.Annotatef(err, reader.toString()+
			"readDataFromHost vol[%v] cannot get  connect from host[%v] request[%v] ",
			reader.key.VolId, host, p.GetUniqLogId())

	}
	defer func() {
		if err != nil {
			log.LogError(err.Error())
		}
		if acatualReadSize>=expectReadSize{
			err=nil
		}
	}()
	if err = p.WriteToConn(conn); err != nil {
		err = errors.Annotatef(err, reader.toString()+"readDataFromHost host[%v] error request[%v]",
			host, p.GetUniqLogId())
		conn.Close()
		return 0, err
	}
	for {
		if acatualReadSize >= expectReadSize {
			break
		}
		err = p.ReadFromConn(conn, proto.ReadDeadlineTime)
		if err != nil {
			err = errors.Annotatef(err, reader.toString()+"readDataFromHost host[%v]  error reqeust[%v]",
				host, p.GetUniqLogId())
			conn.Close()
			return acatualReadSize, err

		}
		if p.ResultCode != proto.OpOk {
			err = errors.Annotatef(fmt.Errorf(string(p.Data[:p.Size])),
				reader.toString()+"readDataFromHost host [%v] request[%v] reply[%v]",
				host, p.GetUniqLogId(), p.GetUniqLogId())
			conn.Close()
			return acatualReadSize, err
		}
		copy(data[acatualReadSize:acatualReadSize+int(p.Size)], p.Data[:p.Size])
		acatualReadSize += int(p.Size)
		if acatualReadSize >= expectReadSize {
			conn.Close()
			return acatualReadSize, err
		}
	}
	reader.wrapper.PutConnect(conn)
	return acatualReadSize, nil
}

func (reader *ExtentReader) updateKey(key proto.ExtentKey) (update bool) {
	reader.Lock()
	defer reader.Unlock()
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
	return fmt.Sprintf("inode[%v] extentKey[%v] start[%v] end[%v]", reader.inode,
		reader.key.Marshal(), reader.startInodeOffset, reader.endInodeOffset)
}

func (reader *ExtentReader) fillCache() error {
	reader.Lock()
	if reader.cache.getBufferEndOffset() == int(reader.key.Size) {
		reader.Unlock()
		return nil
	}
	reader.setCacheToUnavali()
	bufferSize := int(util.Min((int(reader.key.Size) - reader.lastReadOffset),
		DefaultReadBufferSize))
	bufferOffset := reader.lastReadOffset
	p := NewReadPacket(reader.key, bufferOffset, bufferSize)
	reader.Unlock()
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
