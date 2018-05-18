package stream

import (
	"container/list"
	"fmt"
	"net"
	"sync"
	"sync/atomic"

	"github.com/juju/errors"
	"github.com/tiglabs/baudstorage/proto"
	"github.com/tiglabs/baudstorage/sdk"
	"github.com/tiglabs/baudstorage/util"
	"github.com/tiglabs/baudstorage/util/log"
	"time"
)

const (
	CFSBLOCKSIZE  = 64 * util.KB
	CFSEXTENTSIZE = 64 * util.MB

	ContinueRecive         = true
	NotRecive              = false
	ExtentWriterRecoverCnt = 1

	DefaultWriteBufferSize = 1280 * util.KB
)

var (
	FlushErr      = errors.New(" backEndlush error")
	FullExtentErr = errors.New("full Extent")
)

type ExtentWriter struct {
	inode            uint64     //Current write Inode
	requestQueue     *list.List //sendPacketList
	requestQueueLock sync.Mutex
	volGroup         *sdk.VolGroup
	wrapper          *sdk.VolGroupWrapper
	extentId         uint64 //current ExtentId
	currentPacket    *Packet
	seqNo            uint64 //Current Send Packet Seq
	byteAck          uint64 //DataNode Has Ack Bytes
	offset           int
	connect          net.Conn
	handleCh         chan bool //a Chan for signal recive goroutine recive packet from connect
	recoverCnt       int       //if failed,then recover contine,this is recover count

	cond *sync.Cond //flushCond use for backEndlush func
	sync.Mutex
	flushLock sync.Mutex
}

func NewExtentWriter(inode uint64, vol *sdk.VolGroup, wrapper *sdk.VolGroupWrapper, extentId uint64) (writer *ExtentWriter, err error) {
	writer = new(ExtentWriter)
	writer.requestQueue = list.New()
	writer.handleCh = make(chan bool, DefaultWriteBufferSize/(64*util.KB))
	writer.extentId = extentId
	writer.volGroup = vol
	writer.inode = inode
	writer.wrapper = wrapper
	writer.connect, err = wrapper.GetConnect(vol.Hosts[0])
	if err != nil {
		return
	}
	go writer.recive()

	return
}

//when backEndlush func called,and sdk must wait
func (writer *ExtentWriter) flushWait() {
	start := time.Now().UnixNano()
	writer.cond = sync.NewCond(&sync.Mutex{})
	writer.cond.L.Lock()
	go func() {
		writer.cond.L.Lock()
		for {
			if writer.isAllFlushed() || time.Now().UnixNano()-start > int64(time.Second) {
				writer.cond.Signal()
				break
			}
		}
		writer.cond.L.Unlock()

	}()
	writer.cond.Wait()
	writer.cond.L.Unlock()
}

//user call write func
func (writer *ExtentWriter) write(data []byte, size int) (total int, err error) {
	var canWrite int
	defer func() {
		if err != nil {
			writer.getConnect().Close()
			writer.cleanHandleCh()
			err = errors.Annotatef(err, "writer[%v] write failed", writer.toString())
		}
	}()
	for total < size && !writer.isFullExtent() {
		writer.Lock()
		if writer.currentPacket == nil {
			writer.currentPacket = NewWritePacket(writer.volGroup, writer.extentId, writer.getSeqNo(), writer.offset)
			writer.addSeqNo() //init a packet
		}
		canWrite = writer.currentPacket.fill(data[total:size], size-total) //fill this packet
		if writer.IsFullCurrentPacket() || canWrite == 0 {
			writer.Unlock()
			err = writer.sendCurrPacket()        //send packet to datanode
			if err != nil && !writer.recover() { //if failed,recover it
				break
			}
			err = nil
		} else {
			writer.Unlock()
		}
		total += canWrite

	}

	return
}

func (writer *ExtentWriter) IsFullCurrentPacket() bool {
	return writer.currentPacket.isFullPacket()
}

func (writer *ExtentWriter) sendCurrPacket() (err error) {
	writer.Lock()
	if writer.currentPacket == nil {
		writer.Unlock()
		return
	}
	if writer.currentPacket.getPacketLength() == 0 {
		writer.Unlock()
		return
	}
	packet := writer.currentPacket
	writer.pushRequestToQueue(packet)
	writer.currentPacket = nil
	writer.offset += packet.getPacketLength()
	writer.Unlock()
	err = packet.writeTo(writer.connect) //if send packet,then signal recive goroutine for recive from connect
	if err == nil {
		writer.handleCh <- ContinueRecive
		return
	} else {
		writer.cleanHandleCh() //if send packet failed,clean handleCh
	}
	err = errors.Annotatef(err, "sendCurrentPacket Failed")
	log.LogError(err.Error())

	return err
}

//if send failed,recover it
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
	//get connect from volGroupWraper
	if connect, err = writer.wrapper.GetConnect(writer.volGroup.Hosts[0]); err != nil {
		return
	}
	writer.setConnect(connect)
	requests := writer.getRequests() //get sendList Packet,then write it to datanode
	for _, request := range requests {
		err = request.WriteToConn(writer.getConnect())
		if err != nil {
			return
		}
		writer.recoverCnt = 0
		writer.handleCh <- ContinueRecive //signal recive goroutine,recive ack from connect
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

//every Extent is FULL,must is 64MB
func (writer *ExtentWriter) isFullExtent() bool {
	return writer.offset+CFSBLOCKSIZE >= CFSEXTENTSIZE
}

//check allPacket has Ack
func (writer *ExtentWriter) isAllFlushed() bool {
	writer.Lock()
	defer writer.Unlock()
	return !(writer.getQueueListLen() > 0 || writer.currentPacket != nil)
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

func (writer *ExtentWriter) checkIsStopReciveGoRoutine() {
	if writer.isAllFlushed() && writer.isFullExtent() {
		writer.handleCh <- NotRecive
	}
	return
}

func (writer *ExtentWriter) flush() (err error) {
	err = errors.Annotatef(FlushErr, "cannot backEndlush writer")
	defer func() {
		writer.flushLock.Unlock()
		writer.checkIsStopReciveGoRoutine()
		if err == nil {
			return
		}
		err = errors.Annotatef(err, "writer[%v] flush Failed", writer.toString())
		if !writer.recover() {
			return
		}
		err = writer.flush()
	}()
	writer.flushLock.Lock()
	if writer.isAllFlushed() {
		err = nil
		return nil
	}
	if writer.getPacket() != nil {
		if err = writer.sendCurrPacket(); err != nil {
			return err
		}
	}
	if writer.isAllFlushed() {
		err = nil
		return nil
	}
	writer.flushWait()
	if !writer.isAllFlushed() {
		err = errors.Annotatef(FlushErr, "cannot backEndlush writer")
		return err
	}

	return nil
}

func (writer *ExtentWriter) close() {
	if writer.isAllFlushed() {
		writer.handleCh <- NotRecive
	} else {
		writer.flush()
	}

}

func (writer *ExtentWriter) processReply(e *list.Element, request, reply *Packet) (err error) {
	if !request.IsEqual(reply) {
		writer.connect.Close()
		return errors.Annotatef(fmt.Errorf("processReply recive [%v] but actual recive [%v]",
			request.GetUniqLogId(), reply.GetUniqLogId()), "writer[%v]", writer.toString())
	}
	if reply.ResultCode != proto.OpOk {
		writer.connect.Close()
		return errors.Annotatef(fmt.Errorf("processReply recive [%v] error [%v]", request.GetUniqLogId(),
			string(reply.Data[:reply.Size])), "writer[%v]", writer.toString())
	}
	writer.addByteAck(uint64(request.Size))
	writer.removeRquest(e)
	log.LogDebug(fmt.Sprintf("ActionProcessReply[%v] is recived", request.GetUniqLogId()))

	return nil
}

func (writer *ExtentWriter) toKey() (k proto.ExtentKey) {
	k = proto.ExtentKey{}
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

func (writer *ExtentWriter) getNeedRetrySendPackets() (sendList *list.List) {
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
	writer.Lock()
	defer writer.Unlock()
	return writer.currentPacket
}

func (writer *ExtentWriter) setPacket(p *Packet) {
	writer.Lock()
	defer writer.Unlock()
	writer.currentPacket = p
}
