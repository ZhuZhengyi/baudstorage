package stream

import (
	"fmt"
	"sync"
	"time"

	"github.com/juju/errors"
	"github.com/tiglabs/baudstorage/proto"
	"github.com/tiglabs/baudstorage/sdk/data"
	"github.com/tiglabs/baudstorage/util/log"
	"net"
)

const (
	MaxSelectDataPartionForWrite = 32
	ActionGetConnect             = "ActionGetConnect"
	ActionStreamWriteWrite       = "ActionStreamWriteWrite"
	ActionRecoverExtent          = "ActionRecoverExtent"
	IsFlushIng                   = 1
	NoFlushIng                   = -1
)

type WriteRequest struct {
	data     []byte
	size     int
	canWrite int
	err      error
}

type StreamWriter struct {
	sync.Mutex
	wrapper          *data.DataPartionWrapper
	currentWriter    *ExtentWriter //current ExtentWriter
	errCount         int           //error count
	currentPartionId uint32        //current PartionId
	currentExtentId  uint64        //current FileIdId
	currentInode     uint64        //inode
	flushLock        sync.Mutex
	execludePartion  []uint32
	appendExtentKey  AppendExtentKeyFunc
	isFlushIng       int32
	requestCh        chan *WriteRequest
	replyCh          chan *WriteRequest
	exitCh           chan bool
}

func NewStreamWriter(wrapper *data.DataPartionWrapper, inode uint64, appendExtentKey AppendExtentKeyFunc) (stream *StreamWriter) {
	stream = new(StreamWriter)
	stream.wrapper = wrapper
	stream.appendExtentKey = appendExtentKey
	stream.currentInode = inode
	stream.isFlushIng = NoFlushIng
	stream.requestCh = make(chan *WriteRequest, 1000)
	stream.replyCh = make(chan *WriteRequest, 1000)
	stream.exitCh = make(chan bool, 2)
	stream.execludePartion = make([]uint32, 0)
	go stream.server()

	return
}

//get current extent writer
func (stream *StreamWriter) getWriter() (writer *ExtentWriter) {
	stream.Lock()
	defer stream.Unlock()
	return stream.currentWriter
}

//set current extent Writer to null
func (stream *StreamWriter) setWriterToNull() {
	stream.Lock()
	defer stream.Unlock()
	stream.currentWriter = nil
}

//set writer
func (stream *StreamWriter) setWriter(writer *ExtentWriter) {
	stream.Lock()
	defer stream.Unlock()
	stream.currentWriter = writer
}

func (stream *StreamWriter) toString() (m string) {
	currentWriterMsg := ""
	if stream.getWriter() != nil {
		currentWriterMsg = stream.getWriter().toString()
	}
	return fmt.Sprintf("inode[%v] currentDataPartion[%v] currentExtentId[%v]"+
		" errCount[%v]", stream.currentInode, stream.currentPartionId, currentWriterMsg,
		stream.errCount)
}

//stream init,alloc a extent ,select dp and extent
func (stream *StreamWriter) init() (err error) {
	if stream.getWriter() != nil && stream.getWriter().isFullExtent() {
		err = stream.flushCurrExtentWriter()
	}
	if err != nil {
		return errors.Annotatef(err, "WriteInit")
	}
	if stream.getWriter() != nil {
		return
	}
	err = stream.allocateNewExtentWriter()
	if err != nil {
		err = errors.Annotatef(err, "WriteInit AllocNewExtentFailed")
		return err
	}

	return
}

func (stream *StreamWriter) server() {
	ticker := time.Tick(time.Second * 2)
	for {
		select {
		case request := <-stream.requestCh:
			request.canWrite, request.err = stream.write(request.data, request.size)
			stream.replyCh <- request
		case <-stream.exitCh:
			return
		case <-ticker:
			if stream.getWriter() == nil {
				continue
			}
			if stream.isFlushIng == IsFlushIng {
				continue
			}
			stream.flushCurrExtentWriter()
		}
	}
}

func (stream *StreamWriter) write(data []byte, size int) (total int, err error) {
	var write int
	defer func() {
		if err == nil {
			return
		}
		err = errors.Annotatef(err, "Stream[%v] Write", stream.toString())
		log.LogError(errors.ErrorStack(err))
	}()
	for total < size {
		if err = stream.init(); err != nil {
			return
		}
		write, err = stream.getWriter().write(data[total:size], size-total)
		if err != nil && err != FullExtentErr {
			err = stream.recoverExtent()
		}
		if err != nil {
			return
		}
		total += write
	}

	return
}

func (stream *StreamWriter) close() (err error) {
	for i := 0; i < 3; i++ {
		err = stream.flushCurrExtentWriter()
		if err == nil {
			break
		}
	}
	if stream.getWriter() != nil {
		stream.Lock()
		err = stream.currentWriter.close()
		stream.Unlock()
	}
	if err == nil {
		stream.exitCh <- true
	}

	return
}

func (stream *StreamWriter) flushCurrExtentWriter() (err error) {
	defer func() {
		stream.isFlushIng = NoFlushIng
		stream.flushLock.Unlock()
		if err == nil {
			stream.errCount = 0
			return
		}
		stream.errCount++
		if stream.errCount < MaxSelectDataPartionForWrite {
			err = stream.recoverExtent()
			if err == nil {
				err = stream.flushCurrExtentWriter()
			}
		}
	}()
	stream.isFlushIng = IsFlushIng
	stream.flushLock.Lock()
	writer := stream.getWriter()
	if writer == nil {
		err = nil
		return nil
	}
	if err = writer.flush(); err != nil {
		err = errors.Annotatef(err, "writer[%v] Flush Failed", writer.toString())
		return err
	}
	ek := writer.toKey()
	if ek.Size != 0 {
		err = stream.appendExtentKey(stream.currentInode, ek)
	}
	if err != nil {
		err = errors.Annotatef(err, "update to MetaNode fileSize[%v] Failed", ek.Size)
		return err
	}
	if writer.isFullExtent() {
		stream.setWriterToNull()
	}

	return err
}

func (stream *StreamWriter) recoverExtent() (err error) {
	retryPackets := stream.getWriter().getNeedRetrySendPackets()
	for i := 0; i < MaxSelectDataPartionForWrite; i++ {
		stream.execludePartion = append(stream.execludePartion, stream.getWriter().dp.DataPartionID)
		if err = stream.allocateNewExtentWriter(); err != nil {
			err = errors.Annotatef(err, "RecoverExtent Failed")
			continue
		}
		ek := stream.getWriter().toKey()
		if ek.Size != 0 {
			err = stream.appendExtentKey(stream.currentInode, ek)
		}
		if err != nil {
			err = errors.Annotatef(err, "update filesize[%v] to metanode Failed", ek.Size)
			continue
		}
		for _, p := range retryPackets {
			_, err = stream.getWriter().write(p.Data, int(p.Size))
			if err != nil {
				err = errors.Annotatef(err, "RecoverExtent write failed")
				continue
			}
		}
		if err == nil {
			stream.execludePartion = make([]uint32, 0)
			break
		}
	}

	return

}

func (stream *StreamWriter) allocateNewExtentWriter() (err error) {
	var (
		dp       *data.DataPartion
		extentId uint64
		writer   *ExtentWriter
	)
	err = fmt.Errorf("cannot alloct new extent after maxrery")
	for i := 0; i < MaxSelectDataPartionForWrite; i++ {
		if dp, err = stream.wrapper.GetWriteDataPartion(stream.execludePartion); err != nil {
			log.LogErrorf(fmt.Sprintf("ActionAllocNewExtentWriter "+
				"failed on getWriteDataPartion,error[%v] execludeDataPartion[%v]", err.Error(), stream.execludePartion))
			continue
		}
		if extentId, err = stream.createExtent(dp); err != nil {
			log.LogErrorf(fmt.Sprintf("ActionAllocNewExtentWriter "+
				"create Extent,error[%v] execludeDataPartion[%v]", err.Error(), stream.execludePartion))
			continue
		}
		if writer, err = NewExtentWriter(stream.currentInode, dp, stream.wrapper, extentId); err != nil {
			log.LogErrorf(fmt.Sprintf("ActionAllocNewExtentWriter "+
				"NewExtentWriter[%v],error[%v] execludeDataPartion[%v]", extentId, err.Error(), stream.execludePartion))
			continue
		}
		break
	}
	if extentId <= 0 {
		log.LogErrorf(errors.Annotatef(err, "allocateNewExtentWriter").Error())
		return errors.Annotatef(err, "allocateNewExtentWriter")
	}
	stream.currentPartionId = dp.DataPartionID
	stream.currentExtentId = extentId
	stream.setWriter(writer)
	err = nil
	log.LogInfo(fmt.Sprintf("StreamWriter[%v] ActionAllocNewExtentWriter extentId[%v] success", stream.toString(), extentId))

	return nil
}

func (stream *StreamWriter) createExtent(dp *data.DataPartion) (extentId uint64, err error) {
	var (
		connect net.Conn
	)
	connect, err = stream.wrapper.GetConnect(dp.Hosts[0])
	if err != nil {
		err = errors.Annotatef(err, " get connect from datapartionHosts[%v]", dp.Hosts[0])
		return
	}
	p := NewCreateExtentPacket(dp)
	if err = p.WriteToConn(connect); err != nil {
		err = errors.Annotatef(err, "send CreateExtent[%v] to datapartionHosts[%v]", p.GetUniqLogId(), dp.Hosts[0])
		connect.Close()
		return
	}
	if err = p.ReadFromConn(connect, proto.ReadDeadlineTime); err != nil {
		err = errors.Annotatef(err, "recive CreateExtent[%v] failed", p.GetUniqLogId(), dp.Hosts[0])
		connect.Close()
		return
	}
	extentId = p.FileID
	if p.FileID <= 0 {
		err = errors.Annotatef(err, "unavali extentId[%v] from [%v] response",
			extentId, dp.Hosts[0])
		connect.Close()
		return

	}
	stream.wrapper.PutConnect(connect)

	return extentId, nil
}
