package stream

import (
	"fmt"
	"sync"
	"time"

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
)

type StreamWriter struct {
	sync.Mutex
	wraper          *sdk.VolGroupWraper
	keys            *chan ExtentKey
	currentWriter   *ExtentWriter
	errCount        int
	excludeVols     []uint32
	bytesWriten     int
	currentVolId    uint32
	currentExtentId uint64
	currentInode    uint64
}

func NewStreamWriter(wraper *sdk.VolGroupWraper, inode uint64, keys *chan ExtentKey) (stream *StreamWriter) {
	stream = new(StreamWriter)
	stream.excludeVols = make([]uint32, 0)
	stream.keys = keys
	stream.wraper = wraper
	stream.currentInode = inode
	go stream.flush()

	return
}

func (stream *StreamWriter) getWriter() (writer *ExtentWriter) {
	stream.Lock()
	defer stream.Unlock()
	return stream.currentWriter
}

func (stream *StreamWriter) setWriterToNull() {
	stream.Lock()
	defer stream.Unlock()
	stream.currentWriter = nil
}

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
	return fmt.Sprintf("inode[%v] currentVol[%v] currentExtentId[%v] bytesWriten[%v]"+
		" errCount[%v]", stream.currentInode, stream.currentVolId, currentWriterMsg,
		stream.bytesWriten, stream.errCount)
}

func (stream *StreamWriter) init() (err error) {
	if stream.getWriter() != nil && stream.getWriter().isFullExtent() {
		err = stream.flushCurrExtentWriter()
	}
	if err != nil {
		return
	}
	if stream.getWriter() != nil {
		return
	}
	err = stream.allocateNewExtentWriter()
	if err != nil {
		log.LogError(fmt.Sprintf(util.GetFuncTrace()+" AllocNewExtentFailed err[%v]", err.Error()))
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
		log.LogError(fmt.Sprintf(util.GetFuncTrace()+ActionStreamWriteWrite+" failed err[%v]", err.Error()))
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
	err = stream.getWriter().flush()
	if err != nil {
		return
	}
	*stream.keys <- stream.getWriter().toKey()
	if stream.getWriter().isFullExtent() {
		stream.setWriterToNull()
	}

	return
}

func (stream *StreamWriter) recoverExtent() (err error) {
	defer func() {
		if err == nil {
			log.LogInfo(ActionRecoverExtent + stream.currentWriter.toString() + " success")
		} else {
			log.LogError(fmt.Sprintf(ActionRecoverExtent+stream.currentWriter.toString()+" failed[%v]", err))
		}
	}()

	sendList := stream.getWriter().getNeedRetrySendPacket()
	if err = stream.allocateNewExtentWriter(); err != nil {
		return
	}
	*stream.keys <- stream.getWriter().toKey()
	for e := sendList.Front(); e != nil; e = e.Next() {
		p := e.Value.(*Packet)
		_, err = stream.getWriter().write(p.Data, int(p.Size))
		if err != nil {
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
		if vol, err = stream.wraper.GetWriteVol(stream.excludeVols); err != nil {
			continue
		}
		if extentId, err = stream.createExtent(vol); err != nil {
			continue
		}
		if writer, err = NewExtentWriter(stream.currentInode, vol, stream.wraper, extentId); err != nil {
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
	connect, err := stream.wraper.GetConnect(vol.Hosts[0])
	if err != nil {
		log.LogError(fmt.Sprintf(util.GetFuncTrace()+" streamWriter[%v] volhosts[%v]", stream.toString(), vol.Hosts))
		return
	}
	defer func() {
		if err == nil {
			stream.wraper.PutConnect(connect)
		} else {
			connect.Close()
		}
	}()
	p := NewPacket(vol)
	p.Opcode = proto.OpCreateFile
	p.VolID = vol.VolId
	p.Arg = ([]byte)(vol.GetAllAddrs())
	p.Arglen = uint32(len(p.Arg))

	if err = p.WriteToConn(connect); err != nil {
		return
	}
	if err = p.ReadFromConn(connect, proto.ReadDeadlineTime); err != nil {
		return
	}
	extentId = p.FileID

	return
}

func (stream *StreamWriter) flush() {
	ticker := time.Tick(time.Second * 2)
	for {
		select {
		case <-ticker:
			if stream.getWriter() == nil {
				continue
			}
			stream.flushCurrExtentWriter()
		}
	}

}
