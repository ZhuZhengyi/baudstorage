package stream

import (
	"container/list"
	"fmt"
	"github.com/juju/errors"
	"github.com/tiglabs/baudstorage/proto"
	"github.com/tiglabs/baudstorage/sdk"
	"github.com/tiglabs/baudstorage/util"
	"github.com/tiglabs/baudstorage/util/log"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

const (
	CFSBLOCKSIZE  = 64 * util.KB
	CFSEXTENTSIZE = 64 * util.MB

	ContinueRecive         = true
	NotRecive              = false
	ExtentWriterRecoverCnt = 3

	DefaultBufferSize = 1280 * util.KB
)

var (
	FlushErr      = errors.New(" flush error")
	FullExtentErr = errors.New("full Extent")
)

type ExtentWriter struct {
	inode            uint64
	requestQueue     *list.List
	requestQueueLock sync.Mutex
	sync.Mutex
	volGroup      *sdk.VolGroup
	wraper        *sdk.VolGroupWraper
	extentId      uint64
	currentPacket *Packet
	seqNo         uint64
	byteAck       uint64
	offset        int
	connect       net.Conn
	handleCh      chan bool
	recoverCnt    int

	flushCond *sync.Cond
	needFlush bool
}

func NewExtentWriter(inode uint64, vol *sdk.VolGroup, wraper *sdk.VolGroupWraper, extentId uint64) (writer *ExtentWriter, err error) {
	writer = new(ExtentWriter)
	writer.requestQueue = list.New()
	writer.handleCh = make(chan bool, DefaultBufferSize/(64*util.KB))
	writer.extentId = extentId
	writer.volGroup = vol
	writer.inode = inode
	writer.wraper = wraper
	writer.connect, err = wraper.GetConnect(vol.Hosts[0])
	if err != nil {
		return
	}
	go writer.recive()

	return
}

func (writer *ExtentWriter) write(data []byte, size int) (total int, err error) {
	var canWrite int
	defer func() {
		if err != nil {
			writer.getConnect().Close()
			writer.cleanHandleCh()
			err = errors.Annotatef(err, "writer[%v] operator", writer.toString())
		}
	}()
	for total < size && !writer.isFullExtent() {
		if writer.getPacket() == nil {
			writer.addSeqNo()
			writer.setPacket(NewWritePacket(writer.volGroup, writer.extentId, writer.getSeqNo(), writer.offset))
		}
		canWrite = writer.getPacket().fill(data[total:size], total, size-total)
		if writer.getPacket().isFull() {
			err = writer.sendCurrPacket()
			if err != nil && !writer.recover() {
				break
			}
			err = nil
		}
		total += canWrite
	}

	return
}

func (writer *ExtentWriter) sendCurrPacket() (err error) {
	if writer.getPacket() == nil {
		return
	}
	packet := writer.getPacket()
	writer.pushRequestToQueue(packet)
	writer.setPacket(nil)
	writer.offset += packet.getDataLength()
	err = packet.writeTo(writer.connect)
	if err == nil {
		writer.handleCh <- ContinueRecive
		return
	} else {
		writer.cleanHandleCh()
	}
	err = errors.Annotatef(err, "extentWriter[%v] sendCurrentPacket", writer.toString())
	log.LogError(err.Error())

	return err
}

func (writer *ExtentWriter) recover() (sucess bool) {
	if writer.recoverCnt > ExtentWriterRecoverCnt {
		return
	}
	var (
		connect net.Conn
		err     error
	)
	writer.getConnect().Close()
	writer.cleanHandleCh()
	writer.recoverCnt++
	defer func() {
		if err == nil {
			writer.recoverCnt = 0
			return
		}
		writer.getConnect().Close()
		writer.cleanHandleCh()
		err = errors.Annotatef(err, "writer[%v] recover failed", writer.toString())
		log.LogError(err.Error())

	}()
	if connect, err = writer.wraper.GetConnect(writer.volGroup.Hosts[0]); err != nil {
		return
	}
	writer.setConnect(connect)
	requests := writer.getRequests()
	for _, request := range requests {
		err = request.WriteToConn(writer.getConnect())
		if err != nil {
			return
		}
		writer.recoverCnt = 0
		writer.handleCh <- ContinueRecive
	}

	return true
}

func (writer *ExtentWriter) cleanHandleCh() {
	for {
		select {
		case <-writer.handleCh:
			continue
		default:
			return
		}
	}
}

func (writer *ExtentWriter) isFullExtent() bool {
	return writer.offset+CFSBLOCKSIZE >= CFSEXTENTSIZE
}

func (writer *ExtentWriter) isAllFlushed() bool {
	return !(writer.getQueueListLen() > 0 || writer.getPacket() != nil)
}

func (writer *ExtentWriter) toString() string {
	var currPkgMesg string
	if writer.getPacket() != nil {
		currPkgMesg = writer.currentPacket.GetUniqLogId()
	}
	return fmt.Sprintf("Extent{inode=%v volGroup=%v extentId=%v retryCnt=%v handleCh[%v] requestQueueLen[%v] currentPkg=%v}",
		writer.inode, writer.volGroup.VolId, writer.extentId, writer.recoverCnt,
		len(writer.handleCh), writer.getQueueListLen(), currPkgMesg)
}

func (writer *ExtentWriter) checkIsFullExtent() {
	if writer.isAllFlushed() && writer.isFullExtent() {
		writer.handleCh <- NotRecive
	}
	return
}

func (writer *ExtentWriter) flush() (err error) {
	err = errors.Annotatef(FlushErr, "cannot flush writer [%v]", writer.toString())
	log.LogInfo(fmt.Sprintf("ActionFlushExtent [%v] start", writer.toString()))
	defer func() {
		writer.checkIsFullExtent()
		if err == nil {
			log.LogInfo(writer.toString() + " flush ok")
			return
		}
		if !writer.recover() {
			return
		}
		err = writer.flush()
	}()
	if writer.isAllFlushed() {
		err = nil
		return
	}
	if writer.getPacket() != nil {
		err = writer.sendCurrPacket()
		if err != nil {
			return
		}
	}

	if writer.isAllFlushed() {
		err = nil
		return
	}
	writer.flushCond = sync.NewCond(&sync.Mutex{})
	writer.flushCond.L.Lock()
	writer.needFlush = true
	start := time.Now().UnixNano()
	for !writer.isAllFlushed() {
		if time.Now().UnixNano()-start > int64(time.Second*10) {
			break
		}
		writer.flushCond.Wait()
	}
	writer.flushCond.L.Unlock()
	if !writer.isAllFlushed() {
		return errors.Annotatef(FlushErr, "cannot flush writer [%v]", writer.toString())
	}
	if writer.isAllFlushed() {
		err = nil
		return
	}

	return
}

func (writer *ExtentWriter) close() {
	if writer.isAllFlushed() {
		writer.handleCh <- NotRecive
	} else {
		writer.flush()
	}

}

func (writer *ExtentWriter) processReply(e *list.Element, request, reply *Packet) (err error) {
	if !IsEqual(request, reply) {
		writer.connect.Close()
		return errors.Annotatef(fmt.Errorf("processReply recive [%v] but actual recive [%v]",
			request.GetUniqLogId(), reply.GetUniqLogId()), "writer[%v]", writer.toString())
	}
	if reply.Opcode != proto.OpOk {
		writer.connect.Close()
		return errors.Annotatef(fmt.Errorf("processReply recive [%v] error [%v]", request.GetUniqLogId(),
			string(reply.Data[:reply.Size])), "writer[%v]", writer.toString())
	}
	writer.removeRquest(e)
	writer.addByteAck(uint64(request.Size))
	log.LogDebug(fmt.Sprintf("ActionProcessReply[%v] is recived", request.GetUniqLogId()))

	if writer.needFlush && writer.isAllFlushed() {
		writer.flushCond.L.Lock()
		writer.flushCond.Signal()
		writer.needFlush = false
		writer.flushCond.L.Unlock()
	}

	return nil
}

func (writer *ExtentWriter) toKey() (k ExtentKey) {
	k = ExtentKey{}
	k.VolId = writer.volGroup.VolId
	k.Size = uint32(writer.getByteAck())
	k.ExtentId = writer.extentId

	return
}

func (writer *ExtentWriter) recive() {
	for {
		select {
		case code := <-writer.handleCh:
			if code == NotRecive {
				return
			}
			e := writer.getFrontRequest()
			if e == nil {
				continue
			}
			request := e.Value.(*Packet)
			reply := NewReply(request.ReqID, request.VolID, request.FileID)
			err := reply.ReadFromConn(writer.getConnect(), proto.ReadDeadlineTime)
			if err != nil {
				writer.getConnect().Close()
				log.LogError(err)
				continue
			}
			if err = writer.processReply(e, request, reply); err != nil {
				log.LogError(err.Error())
			}
		}
	}
}

func (writer *ExtentWriter) addSeqNo() {
	atomic.AddUint64(&writer.seqNo, 1)
}

func (writer *ExtentWriter) getSeqNo() uint64 {
	return atomic.LoadUint64(&writer.seqNo)
}

func (writer *ExtentWriter) addByteAck(size uint64) {
	atomic.AddUint64(&writer.byteAck, size)
}

func (writer *ExtentWriter) getByteAck() uint64 {
	return atomic.LoadUint64(&writer.byteAck)
}

func (writer *ExtentWriter) getConnect() net.Conn {
	writer.Lock()
	defer writer.Unlock()

	return writer.connect
}

func (writer *ExtentWriter) setConnect(connect net.Conn) {
	writer.Lock()
	defer writer.Unlock()
	writer.connect = connect
}

func (writer *ExtentWriter) getFrontRequest() (e *list.Element) {
	writer.requestQueueLock.Lock()
	defer writer.requestQueueLock.Unlock()
	return writer.requestQueue.Front()
}

func (writer *ExtentWriter) getRequests() (requests []*Packet) {
	writer.requestQueueLock.Lock()
	defer writer.requestQueueLock.Unlock()
	requests = make([]*Packet, 0)
	for e := writer.requestQueue.Front(); e != nil; e = e.Next() {
		requests = append(requests, e.Value.(*Packet))
	}
	return
}

func (writer *ExtentWriter) pushRequestToQueue(request *Packet) {
	writer.requestQueueLock.Lock()
	defer writer.requestQueueLock.Unlock()
	writer.requestQueue.PushBack(request)
}

func (writer *ExtentWriter) removeRquest(e *list.Element) {
	writer.requestQueueLock.Lock()
	defer writer.requestQueueLock.Unlock()
	writer.requestQueue.Remove(e)
}

func (writer *ExtentWriter) getQueueListLen() (length int) {
	writer.requestQueueLock.Lock()
	defer writer.requestQueueLock.Unlock()
	return writer.requestQueue.Len()
}

func (writer *ExtentWriter) getNeedRetrySendPacket() (sendList *list.List) {
	writer.requestQueueLock.Lock()
	sendList = writer.requestQueue
	lastPacket := writer.currentPacket
	if lastPacket != nil && lastPacket.ReqID > sendList.Back().Value.(*Packet).ReqID {
		sendList.PushBack(lastPacket)
	}
	writer.requestQueueLock.Unlock()
	return
}

func (writer *ExtentWriter) getPacket() (p *Packet) {
	return writer.currentPacket
}

func (writer *ExtentWriter) setPacket(p *Packet) {
	writer.currentPacket = p
}
