package datanode

import (
	"container/list"
	"fmt"
	"github.com/juju/errors"
	"net"
	"sync"
)

var single = struct{}{}

var ErrReplyIDLessThanReqID = errors.New("ReplyIDLessThanReqIDErr")

type MessageHandler struct {
	listMux   sync.RWMutex
	sentList  *list.List
	handleCh  chan struct{}
	requestCh chan *Packet
	replyCh   chan *Packet
	inConn    *net.TCPConn
	exitCh    chan bool
	isClean   bool
}

func NewMsgHandler(inConn *net.TCPConn) *MessageHandler {
	m := new(MessageHandler)
	m.sentList = list.New()
	m.handleCh = make(chan struct{}, RequestChanSize)
	m.requestCh = make(chan *Packet, RequestChanSize)
	m.replyCh = make(chan *Packet, RequestChanSize)
	m.exitCh = make(chan bool, 100)
	m.inConn = inConn

	return m
}

func (msgH *MessageHandler) RenewList(isHeadNode bool) {
	if !isHeadNode {
		msgH.sentList = list.New()
	}
}

func (msgH *MessageHandler) ExitSign() {
	for i := 0; i < 20; i++ {
		select {
		case msgH.exitCh <- true:
		default:
			break
		}

	}
}

func (msgH *MessageHandler) checkReplyAvail(reply *Packet) (err error) {
	msgH.listMux.Lock()
	defer msgH.listMux.Unlock()

	for e := msgH.sentList.Front(); e != nil; e = e.Next() {
		request := e.Value.(*Packet)
		if reply.ReqID == request.ReqID {
			return
		}
		return fmt.Errorf(ActionCheckReplyAvail+" expect recive %v but recive %v from %v localaddr %v"+
			" remoteaddr %v", request.GetUniqLogId(), reply.GetUniqLogId(), request.nextAddr,
			request.nextConn.LocalAddr().String(), request.nextConn.RemoteAddr().String())
	}

	return
}

func (msgH *MessageHandler) GetListElement() (e *list.Element) {
	msgH.listMux.RLock()
	e = msgH.sentList.Front()
	msgH.listMux.RUnlock()

	return
}

func (msgH *MessageHandler) PushListElement(e *Packet) {
	msgH.listMux.Lock()
	msgH.sentList.PushBack(e)
	msgH.listMux.Unlock()
}

func (msgH *MessageHandler) ClearReqs(s *DataNode) {
	msgH.listMux.Lock()
	for e := msgH.sentList.Front(); e != nil; e = e.Next() {
		if e.Value.(*Packet).nextAddr != "" && !e.Value.(*Packet).IsTailNode() {
			s.headNodePutChunk(e.Value.(*Packet))
			s.CleanConn(e.Value.(*Packet).nextConn, ForceClostConnect)
		}
	}
	replys := len(msgH.replyCh)
	for i := 0; i < replys; i++ {
		<-msgH.replyCh
	}
	msgH.listMux.Unlock()
}

func (msgH *MessageHandler) ClearReplys() {
	replys := len(msgH.replyCh)
	for i := 0; i < replys; i++ {
		<-msgH.replyCh
	}
}

func (msgH *MessageHandler) DelListElement(reply *Packet, e *list.Element, s *DataNode) (exit bool) {
	msgH.listMux.Lock()
	defer msgH.listMux.Unlock()
	for e := msgH.sentList.Front(); e != nil; e = e.Next() {
		request := e.Value.(*Packet)
		if reply.ReqID == request.ReqID && reply.VolID == request.VolID &&
			reply.FileID == request.FileID && reply.Offset == request.Offset {
			msgH.sentList.Remove(e)
			s.CleanConn(request.nextConn, NOClostConnect)
			pkg := e.Value.(*Packet)
			msgH.replyCh <- pkg
			break
		} else {
			break
		}
	}

	return
}
