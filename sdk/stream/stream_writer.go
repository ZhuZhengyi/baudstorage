package stream

import (
	"fmt"
	"sync"
	"time"

	"github.com/juju/errors"
	"github.com/tiglabs/baudstorage/proto"
	"github.com/tiglabs/baudstorage/sdk"
	"github.com/tiglabs/baudstorage/util"
	"github.com/tiglabs/baudstorage/util/log"
)

const (
	MaxSelectVolForWrite   = 32
	ActionGetConnect       = "ActionGetConnect"
	ActionStreamWriteWrite = "ActionStreamWriteWrite"
	ActionRecoverExtent    = "ActionRecoverExtent"
	IsFlushIng             = 1
	NoFlushIng             = -1
)

type StreamWriter struct {
	sync.Mutex
	wrapper         *sdk.VolGroupWrapper
	currentWriter   *ExtentWriter //current ExtentWriter
	errCount        int           //error count
	excludeVols     []uint32      //exclude Vols
	currentVolId    uint32        //current VolId
	currentExtentId uint64        //current ExtentId
	currentInode    uint64        //inode
	flushLock       sync.Mutex
	appendExtentKey AppendExtentKeyFunc
	isFlushIng      int32
}

func NewStreamWriter(wrapper *sdk.VolGroupWrapper, inode uint64, appendExtentKey AppendExtentKeyFunc) (stream *StreamWriter) {
	stream = new(StreamWriter)
	stream.excludeVols = make([]uint32, 0)
	stream.wrapper = wrapper
	stream.appendExtentKey = appendExtentKey
	stream.currentInode = inode
	stream.isFlushIng = NoFlushIng
	go stream.autoFlushThread()

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
	return fmt.Sprintf("inode[%v] currentVol[%v] currentExtentId[%v]"+
		" errCount[%v]", stream.currentInode, stream.currentVolId, currentWriterMsg,
		stream.errCount)
}

//stream init,alloc a extent ,select vol and extent
func (stream *StreamWriter) init() (err error) {
	if stream.getWriter() != nil && stream.getWriter().isFullExtent() {
		err = stream.flushCurrExtentWriter()
	}
	if err != nil {
		return errors.Annotatef(err, "StreamInfo[%v] WriteInit", stream.toString())
	}
	if stream.getWriter() != nil {
		return
	}
	err = stream.allocateNewExtentWriter()
	if err != nil {
		errors.Annotatef(err, "StreamInfo[%v] WriteInit AllocNewExtentFailed", stream.toString())
		return
	}

	return
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

func (stream *StreamWriter) flushCurrExtentWriter() (err error) {
	defer func() {
		stream.isFlushIng = NoFlushIng
		stream.flushLock.Unlock()
		if err == nil {
			stream.errCount = 0
			return
		}
		stream.errCount++
		if stream.errCount < MaxSelectVolForWrite {
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
	defer func() {
		if err != nil {
			log.LogError(fmt.Sprintf(ActionRecoverExtent+stream.currentWriter.toString()+" failed[%v]", errors.ErrorStack(err)))
		}
	}()

	sendList := stream.getWriter().getNeedRetrySendPackets()
	if err = stream.allocateNewExtentWriter(); err != nil {
		err = errors.Annotatef(err, "RecoverExtent Failed")
		return
	}
	ek := stream.getWriter().toKey()
	if ek.Size != 0 {
		err = stream.appendExtentKey(stream.currentInode, ek)
	}
	if err != nil {
		err = errors.Annotatef(err, "update filesize[%v] to metanode Failed", ek.Size)
		return
	}
	for e := sendList.Front(); e != nil; e = e.Next() {
		p := e.Value.(*Packet)
		_, err = stream.getWriter().write(p.Data, int(p.Size))
		if err != nil {
			err = errors.Annotatef(err, "RecoverExtent write failed")
			return
		}
	}

	return

}

func (stream *StreamWriter) allocateNewExtentWriter() (err error) {
	var (
		vol      *sdk.VolGroup
		extentId uint64
		writer   *ExtentWriter
	)
	err = fmt.Errorf("cannot alloct new extent after maxrery")
	for i := 0; i < MaxSelectVolForWrite; i++ {
		if vol, err = stream.wrapper.GetWriteVol(stream.excludeVols); err != nil {
			continue
		}
		if extentId, err = stream.createExtent(vol); err != nil {
			continue
		}
		if writer, err = NewExtentWriter(stream.currentInode, vol, stream.wrapper, extentId); err != nil {
			continue
		}
		break
	}
	if err != nil {
		return
	}
	stream.currentVolId = vol.VolId
	stream.currentExtentId = extentId
	stream.setWriter(writer)
	err = nil
	log.LogInfo(fmt.Sprintf("StreamWriter[%v] ActionAllocNewExtentWriter success", stream.toString()))

	return
}

func (stream *StreamWriter) createExtent(vol *sdk.VolGroup) (extentId uint64, err error) {
	connect, err := stream.wrapper.GetConnect(vol.Hosts[0])
	if err != nil {
		err = errors.Annotatef(err, " streamWriter[%v] get connect from volhosts[%v]", stream.toString(), vol.Hosts[0])
		return
	}
	defer func() {
		if err == nil {
			stream.wrapper.PutConnect(connect)
		} else {
			connect.Close()
		}
	}()
	p := NewCreateExtentPacket(vol)
	if err = p.WriteToConn(connect); err != nil {
		err = errors.Annotatef(err, "send CreateExtent[%v] to volhosts[%v]", p.GetUniqLogId(), vol.Hosts[0])
		return
	}
	if err = p.ReadFromConn(connect, proto.ReadDeadlineTime); err != nil {
		err = errors.Annotatef(err, "recive CreateExtent[%v] failed", p.GetUniqLogId(), vol.Hosts[0])
		return
	}
	extentId = p.FileID

	return
}

func (stream *StreamWriter) autoFlushThread() {
	ticker := time.Tick(time.Second * 2)
	for {
		select {
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
