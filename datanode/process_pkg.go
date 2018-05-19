package datanode

import (
	"container/list"
	"fmt"
	"github.com/tiglabs/baudstorage/proto"
	"github.com/tiglabs/baudstorage/storage"
	"github.com/tiglabs/baudstorage/util/log"
	"golang.org/x/text/cmd/gotext/examples/extract_http/pkg"
	"hash/crc32"
	"io"
	"net"
	"time"
)

func (s *DataNode) readFromCliAndDeal(msgH *MessageHandler) (err error) {
	pkg := NewPacket()
	s.statsFlow(pkg, InFlow)
	remote := msgH.inConn.RemoteAddr().String()
	if err = pkg.ReadFromConn(msgH.inConn, proto.NoReadDeadlineTime); err != nil {
		goto errDeal
	}

	if err = s.CheckPacket(pkg); err != nil {
		goto errDeal
	}
	if err = s.checkAndAddInfos(pkg); err != nil {
		msgH.replyCh <- pkg
		return nil
	}
	msgH.requestCh <- pkg

	return nil
errDeal:
	conn_tag := fmt.Sprintf("connection[%v <----> %v] ", msgH.inConn.LocalAddr, msgH.inConn.RemoteAddr)
	if err == io.EOF {
		err = fmt.Errorf("%v was closed by peer[%v]", conn_tag, remote)
	}
	if err == nil {
		err = fmt.Errorf("msghandler(%v) requestCh is full requestChan len [%v]", conn_tag, len(msgH.requestCh))
	}
	msgH.ExitSign()

	return

}

func (s *DataNode) checkAndAddInfos(pkg *Packet) error {
	var err error
	switch pkg.StoreMode {
	case proto.TinyStoreMode:
		err = s.handleChunkInfo(pkg)
	case proto.ExtentStoreMode:
		if pkg.isHeadNode() && pkg.Opcode == proto.OpCreateFile {
			pkg.FileID = pkg.vol.store.(*storage.ExtentStore).GetExtentId()
		}
	}
	return err
}

func (s *DataNode) handleReqs(msgH *MessageHandler) {
	for {
		select {
		case <-msgH.handleCh:
			pkg, exit := s.receiveFromNext(msgH)
			s.headNodePutChunk(pkg)
			if exit {
				msgH.ExitSign()
				log.LogError(fmt.Sprintf("recive from next notifyall because [%v] exit [%v]", pkg.GetUniqLogId(),
					msgH.inConn.LocalAddr().String()))
			}

		case <-msgH.exitCh:
			return
		}
	}
}

func (s *DataNode) doRequestCh(req *Packet, msgH *MessageHandler) {
	var err error
	if !req.IsTransitPkg() {
		s.operatePacket(req, msgH.inConn)
		msgH.replyCh <- req
		return
	}

	if err = s.sendToNext(req, msgH); err == nil {
		s.operatePacket(req, msgH.inConn)
	} else {
		log.LogError(req.ActionMesg(ActionSendToNext, req.nextAddr,
			req.StartT, fmt.Errorf("failed to send to : %v", req.nextAddr)))
		if req.IsMarkDeleteReq() {
			s.operatePacket(req, msgH.inConn)
		}
	}
	msgH.handleCh <- single

	return
}

func (s *DataNode) doReplyCh(reply *Packet, msgH *MessageHandler) {
	var err error
	if reply.IsErrPack() {
		err = fmt.Errorf(reply.ActionMesg(ActionWriteToCli, msgH.inConn.RemoteAddr().String(),
			reply.StartT, fmt.Errorf(string(reply.Data[:reply.Size]))))
		log.LogError(err)
	}

	if err = reply.WriteToConn(msgH.inConn); err != nil {
		err = fmt.Errorf(reply.ActionMesg(ActionWriteToCli, msgH.inConn.RemoteAddr().String(),
			reply.StartT, err))
		log.LogError(err)
		msgH.ExitSign()
	}
	log.LogDebug(reply.ActionMesg(ActionWriteToCli,
		msgH.inConn.RemoteAddr().String(), reply.StartT, err))
	s.statsFlow(reply, OutFlow)
}

func (s *DataNode) writeToCli(msgH *MessageHandler) {
	for {
		select {
		case req := <-msgH.requestCh:
			s.doRequestCh(req, msgH)
		case reply := <-msgH.replyCh:
			s.doReplyCh(reply, msgH)
		case <-msgH.exitCh:
			msgH.ClearReqs(s)
			return
		}
	}
}

func (s *DataNode) receiveFromNext(msgH *MessageHandler) (request *Packet, exit bool) {
	var (
		err   error
		e     *list.Element
		reply *Packet
	)
	if e = msgH.GetListElement(); e == nil {
		return
	}

	request = e.Value.(*Packet)
	defer func() {
		exit = msgH.DelListElement(request.ReqID, request.VolID, e, s)
		s.statsFlow(request, OutFlow)
		s.statsFlow(reply, InFlow)
	}()
	if request.nextConn == nil {
		request.PackErrorBody(ActionReciveFromNext, ConnIsNullErr)
		return
	}

	//if local excute failed,then
	if request.IsErrPack() {
		request.PackErrorBody(ActionReciveFromNext, request.getErr())
		log.LogError(request.ActionMesg(ActionReciveFromNext, LocalProcessAddr, request.StartT, fmt.Errorf(request.getErr())))
		return
	}

	reply = NewPacket()
	if err = reply.ReadFromConn(request.nextConn, proto.ReadDeadlineTime); err == nil {
		if reply.ReqID == request.ReqID && reply.VolID == request.VolID {
			goto sucess
		}
		if err = msgH.checkReplyAvail(reply); err != nil {
			log.LogError(err.Error())
			return request, true
		}
	} else {
		log.LogError(request.ActionMesg(ActionReciveFromNext, request.nextAddr, request.StartT, err))
		request.PackErrorBody(ActionReciveFromNext, err.Error())
		return
	}

	return

sucess:
	if reply.IsErrPack() {
		err = fmt.Errorf(ActionReciveFromNext+"[%v] remote [%v] do failed[%v]", request.GetUniqLogId(),
			request.nextAddr, string(reply.Data[:reply.Size]))
		request.CopyFrom(reply)
		request.PackErrorBody(ActionReciveFromNext, err.Error())
		log.LogError(err.Error())
	}
	log.LogDebug(reply.ActionMesg(ActionReciveFromNext, request.nextAddr, request.StartT, err))

	return
}

func (s *DataNode) sendToNext(pkg *Packet, msgH *MessageHandler) error {
	var err error
	msgH.PushListElement(pkg)
	pkg.nextConn, err = s.GetNextConn(pkg.nextAddr)
	pkg.Nodes--
	if err == nil {
		err = pkg.WriteToConn(pkg.nextConn)
	}
	pkg.Nodes++
	if err != nil {
		msg := fmt.Sprintf("pkg inconnect[%v] to[%v] err[%v]", msgH.inConn.RemoteAddr().String(), pkg.nextAddr, err.Error())
		pkg.PackErrorBody(ActionSendToNext, msg)
	}

	return err
}

func (s *DataNode) CheckStoreMode(p *Packet) (err error) {
	if p.StoreMode == proto.TinyStoreMode || p.StoreMode == proto.ExtentStoreMode {
		return nil
	}
	return ErrStoreTypeUnmatch
}

func (s *DataNode) CheckPacket(pkg *Packet) error {
	var err error
	pkg.StartT = time.Now().UnixNano()
	if err = s.CheckStoreMode(pkg); err != nil {
		return err
	}

	if err = CheckCrc(pkg); err != nil {
		return err
	}
	var addrs []string
	if addrs, err = pkg.UnmarshalAddrs(); err == nil {
		err = pkg.GetNextAddr(addrs)
	}
	if err != nil {
		return err
	}
	pkg.vol = s.space.getVol(pkg.VolID)
	if pkg.vol == nil {
		return ErrVolNotExist
	}

	return nil
}

func CheckCrc(p *Packet) (err error) {
	if !p.IsWriteOperation() {
		return
	}

	crc := crc32.ChecksumIEEE(p.Data[:p.Size])
	if crc == p.Crc {
		return
	}

	return storage.ErrPkgCrcUnmatch
}

func (s *DataNode) statsFlow(pkg *Packet, flag bool) {
	if pkg == nil {
		return
	}
	if flag == OutFlow {
		s.stats.AddInDataSize(uint64(pkg.Size + pkg.Arglen))
		return
	}

	if pkg.IsReadReq() {
		s.stats.AddInDataSize(uint64(pkg.Arglen))
	} else {
		s.stats.AddInDataSize(uint64(pkg.Size + pkg.Arglen))
	}

}

func (s *DataNode) GetNextConn(nextAddr string) (conn net.Conn, err error) {
	return s.ConnPool.Get(nextAddr)
}

func (s *DataNode) CleanConn(conn net.Conn, isForceClost bool) {
	if conn == nil {
		return
	}
	if isForceClost {
		conn.Close()
		return
	}
	s.ConnPool.Put(conn)
}
