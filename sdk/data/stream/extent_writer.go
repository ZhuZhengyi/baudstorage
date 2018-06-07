package stream

import (
	"container/list"
	"fmt"
	"net"
	"sync"
	"sync/atomic"

	"github.com/juju/errors"
	"github.com/tiglabs/baudstorage/proto"
	"github.com/tiglabs/baudstorage/sdk/data"
	"github.com/tiglabs/baudstorage/util"
	"github.com/tiglabs/baudstorage/util/log"
	"time"
)

const (
	BlockSize  = 64 * util.KB
	ExtentSize = 64 * util.MB

	ContinueReceive        = true
	NotReceive             = false
	ExtentWriterRecoverCnt = 0

	DefaultWriteBufferSize = 2 * util.MB
)

var (
	FlushErr      = errors.New("backend flush error")
	FullExtentErr = errors.New("full extent")
)

type ExtentWriter struct {
	inode            uint64     //Current write Inode
	requestQueue     *list.List //sendPacketList
	requestQueueLock sync.Mutex
	dp               *data.DataPartition
	w          *data.Wrapper
	extentId         uint64 //current FileIdId
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

func NewExtentWriter(inode uint64, dp *data.DataPartition, w *data.Wrapper, extentId uint64) (writer *ExtentWriter, err error) {
	if extentId <= 0 {
		return nil, fmt.Errorf("inode[%v],dp[%v],unavalid extentId[%v]", inode, dp.PartitionID, extentId)
	}
	writer = new(ExtentWriter)
	writer.requestQueue = list.New()
	writer.handleCh = make(chan bool, DefaultWriteBufferSize/(64*util.KB))
	writer.extentId = extentId
	writer.dp = dp
	writer.inode = inode
	writer.w = w
	writer.connect, err = w.GetConnect(dp.Hosts[0])
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
			writer.currentPacket = NewWritePacket(writer.dp, writer.extentId, writer.getSeqNo(), writer.offset)
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
		writer.handleCh <- ContinueReceive
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
	if writer.recoverCnt >= ExtentWriterRecoverCnt {
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
	// get connect from w
	if connect, err = writer.w.GetConnect(writer.dp.Hosts[0]); err != nil {
		log.LogError(err)
		return
	}
	writer.setConnect(connect)
	requests := writer.getRequests() //get sendList Packet,then write it to datanode
	for _, request := range requests {
		err = request.WriteToConn(writer.getConnect())
		if err != nil {
			log.LogError(err)
			return
		}
		writer.recoverCnt = 0
		writer.handleCh <- ContinueReceive //signal recive goroutine,recive ack from connect
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

//every extent is FULL,must is 64MB
func (writer *ExtentWriter) isFullExtent() bool {
	return writer.offset+BlockSize >= ExtentSize
}

//check allPacket has Ack
func (writer *ExtentWriter) isAllFlushed() bool {
	writer.Lock()
	defer writer.Unlock()
	return !(writer.getQueueListLen() > 0 || writer.currentPacket != nil)
}

func (writer *ExtentWriter) toString() string {
	var currPkgMesg string
	writer.Lock()
	if writer.currentPacket != nil {
		currPkgMesg = writer.currentPacket.GetUniqLogId()
	}
	writer.Unlock()
	return fmt.Sprintf("extent{inode=%v dp=%v extentId=%v retryCnt=%v handleCh[%v] requestQueueLen[%v] currentPkg=%v}",
		writer.inode, writer.dp.PartitionID, writer.extentId, writer.recoverCnt,
		len(writer.handleCh), writer.getQueueListLen(), currPkgMesg)
}

func (writer *ExtentWriter) checkIsStopReciveGoRoutine() {
	if writer.isAllFlushed() && writer.isFullExtent() {
		writer.handleCh <- NotReceive
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
		err = errors.Annotatef(err, "flush Failed")
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

func (writer *ExtentWriter) close() (err error) {
	if writer.isAllFlushed() {
		writer.handleCh <- NotReceive
	} else {
		err = writer.flush()
		if err == nil && writer.isAllFlushed() {
			writer.handleCh <- NotReceive
		}
	}
	return
}

func (writer *ExtentWriter) processReply(e *list.Element, request, reply *Packet) (err error) {
	if !request.IsEqual(reply) {
		writer.connect.Close()
		return errors.Annotatef(fmt.Errorf("processReply recive [%v] but actual recive [%v]",
			reply.GetUniqLogId(), request.GetUniqLogId()), "writer[%v]", writer.toString())
	}
	if reply.ResultCode != proto.OpOk {
		writer.connect.Close()
		return errors.Annotatef(fmt.Errorf("processReply recive [%v] error [%v] request[%v]", reply.GetUniqLogId(),
			string(reply.Data[:reply.Size]), request.GetUniqLogId()), "writer[%v]", writer.toString())
	}
	writer.addByteAck(uint64(request.Size))
	writer.removeRquest(e)
	orgReplySize := reply.Size
	reply.Size = request.Size
	log.LogDebug(fmt.Sprintf("ActionProcessReply[%v] is recived Writer [%v]",
		reply.GetUniqLogId(), writer.toString()))
	reply.Size = orgReplySize

	return nil
}

func (writer *ExtentWriter) toKey() (k proto.ExtentKey) {
	k = proto.ExtentKey{}
	k.PartitionId = writer.dp.PartitionID
	k.Size = uint32(writer.getByteAck())
	k.ExtentId = writer.extentId

	return
}

func (writer *ExtentWriter) recive() {
	for {
		select {
		case code := <-writer.handleCh:
			if code == NotReceive {
				return
			}
			e := writer.getFrontRequest()
			if e == nil {
				continue
			}
			request := e.Value.(*Packet)
			reply := NewReply(request.ReqID, request.PartitionID, request.FileID)
			reply.Opcode = request.Opcode
			reply.Offset = request.Offset
			reply.Size = request.Size
			err := reply.ReadFromConn(writer.getConnect(), proto.ReadDeadlineTime)
			if err != nil {
				writer.getConnect().Close()
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

func (writer *ExtentWriter) getNeedRetrySendPackets() (requests []*Packet) {
	var (
		backPkg *Packet
	)
	writer.requestQueueLock.Lock()
	defer writer.requestQueueLock.Unlock()
	writer.Lock()
	defer writer.Unlock()
	requests = make([]*Packet, 0)
	for e := writer.requestQueue.Front(); e != nil; e = e.Next() {
		requests = append(requests, e.Value.(*Packet))
	}
	currentPkg := writer.currentPacket
	if currentPkg == nil {
		return
	}
	if len(requests) == 0 {
		requests = append(requests, currentPkg)
		return
	}
	backPkg = requests[len(requests)-1]
	if currentPkg.ReqID > backPkg.ReqID {
		requests = append(requests, currentPkg)
	}

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
