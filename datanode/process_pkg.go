package datanode

import (
	"container/list"
	"fmt"
	"github.com/juju/errors"
	"github.com/tiglabs/baudstorage/proto"
	"github.com/tiglabs/baudstorage/storage"
	"github.com/tiglabs/baudstorage/util/log"
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
	if pkg.IsMasterCommand() {
		msgH.requestCh <- pkg
		return
	}
	pkg.beforeTp(s.clusterId)

	if err = s.CheckPacket(pkg); err != nil {
		goto errDeal
	}
	if err = s.checkAndAddInfo(pkg); err != nil {
		msgH.replyCh <- pkg
		return nil
	}
	msgH.requestCh <- pkg

	return nil
errDeal:
	connTag := fmt.Sprintf("connection[%v <----> %v] ", msgH.inConn.LocalAddr(), msgH.inConn.RemoteAddr())
	if err == io.EOF {
		err = fmt.Errorf("%v was closed by peer[%v]", connTag, remote)
	}
	if err == nil {
		err = fmt.Errorf("msghandler(%v) requestCh is full requestChan len [%v]", connTag, len(msgH.requestCh))
	}
	msgH.ExitSign()

	return

}

func (s *DataNode) checkAndAddInfo(pkg *Packet) error {
	var err error
	switch pkg.StoreMode {
	case proto.TinyStoreMode:
		err = s.handleChunkInfo(pkg)
	case proto.ExtentStoreMode:
		if pkg.isHeadNode() && pkg.Opcode == proto.OpCreateFile {
			pkg.FileID = pkg.dataPartition.store.(*storage.ExtentStore).GetExtentId()
			log.LogDebugf("action[DataNode.checkAndAddInfo] pkg[%v] alloc fileId[%v]", pkg.GetUniqLogId(), pkg.FileID)
		}
	}
	return err
}

func (s *DataNode) handleRequest(msgH *MessageHandler) {
	for {
		select {
		case <-msgH.handleCh:
			pkg, exit := s.receiveFromNext(msgH)
			s.headNodePutChunk(pkg)
			if exit {
				msgH.ExitSign()
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
		if !(req.Opcode == proto.OpStreamRead) {
			msgH.replyCh <- req
		}

		return
	}

	if err = s.sendToNext(req, msgH); err == nil {
		s.operatePacket(req, msgH.inConn)
	} else {
		log.LogErrorf("action[DataNode.doRequestCh] %dp.", req.ActionMsg(ActionSendToNext, req.nextAddr,
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
		err = fmt.Errorf(reply.ActionMsg(ActionWriteToCli, msgH.inConn.RemoteAddr().String(),
			reply.StartT, fmt.Errorf(string(reply.Data[:reply.Size]))))
		log.LogErrorf("action[DataNode.doReplyCh] %v", err)
	}

	if reply.Opcode != proto.OpStreamRead {
		if err = reply.WriteToConn(msgH.inConn); err != nil {
			err = fmt.Errorf(reply.ActionMsg(ActionWriteToCli, msgH.inConn.RemoteAddr().String(),
				reply.StartT, err))
			log.LogErrorf("action[DataNode.doReplyCh] %v", err)
			msgH.ExitSign()
		}
	}
	if !reply.IsMasterCommand() {
		reply.afterTp()
		log.LogDebugf("action[DataNode.doReplyCh] %v", reply.ActionMsg(ActionWriteToCli,
			msgH.inConn.RemoteAddr().String(), reply.StartT, err))
		s.statsFlow(reply, OutFlow)
	}
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
		s.statsFlow(request, OutFlow)
		s.statsFlow(reply, InFlow)
	}()
	if request.nextConn == nil {
		err = errors.Annotatef(fmt.Errorf(ConnIsNullErr), "Request[%v] receiveFromNext Error", request.GetUniqLogId())
		request.PackErrorBody(ActionReceiveFromNext, err.Error())
		msgH.DelListElement(request, e, s, ForceCloseConnect)
		return
	}

	//if local execute failed,then
	if request.IsErrPack() {
		err = errors.Annotatef(fmt.Errorf(request.getErr()), "Request[%v] receiveFromNext Error", request.GetUniqLogId())
		request.PackErrorBody(ActionReceiveFromNext, err.Error())
		msgH.DelListElement(request, e, s, ForceCloseConnect)
		log.LogErrorf("action[DataNode.receiveFromNext] %dp.", request.ActionMsg(ActionReceiveFromNext, LocalProcessAddr, request.StartT, fmt.Errorf(request.getErr())))
		return
	}

	reply = NewPacket()
	if err = reply.ReadFromConn(request.nextConn, proto.ReadDeadlineTime); err == nil {
		if reply.ReqID == request.ReqID && reply.PartitionID == request.PartitionID && request.Offset == reply.Offset {
			goto success
		}
		if err = msgH.checkReplyAvail(reply); err != nil {
			request.PackErrorBody(ActionReceiveFromNext, err.Error())
			msgH.DelListElement(request, e, s, ForceCloseConnect)
			log.LogErrorf("action[DataNode.receiveFromNext] %dp.", err.Error())
			return request, true
		}
	} else {
		log.LogErrorf("action[DataNode.receiveFromNext] %dp.", request.ActionMsg(ActionReceiveFromNext, request.nextAddr, request.StartT, err))
		err = errors.Annotatef(err, "Request[%v] receiveFromNext Error", request.GetUniqLogId())
		request.PackErrorBody(ActionReceiveFromNext, err.Error())
		msgH.DelListElement(request, e, s, ForceCloseConnect)
		return
	}

	return

success:
	if reply.IsErrPack() {
		err = fmt.Errorf(ActionReceiveFromNext+"remote [%v] do failed[%v]",
			request.nextAddr, string(reply.Data[:reply.Size]))
		err = errors.Annotatef(err, "Request[%v] receiveFromNext Error", request.GetUniqLogId())
		request.CopyFrom(reply)
		request.PackErrorBody(ActionReceiveFromNext, err.Error())
	}
	msgH.DelListElement(request, e, s, NOCloseConnect)
	log.LogDebugf("action[DataNode.receiveFromNext] %dp.", reply.ActionMsg(ActionReceiveFromNext, request.nextAddr, request.StartT, err))

	return
}

func (s *DataNode) sendToNext(pkg *Packet, msgH *MessageHandler) error {
	var (
		err      error
		nextConn *net.TCPConn
	)
	msgH.PushListElement(pkg)
	nextConn, err = gConnPool.Get(pkg.nextAddr)
	if err != nil {
		return err
	}
	pkg.nextConn = nextConn
	pkg.Nodes--
	if err == nil {
		err = pkg.WriteToConn(pkg.nextConn)
	}
	pkg.Nodes++
	if err != nil {
		msg := fmt.Sprintf("pkg inconnect[%v] to[%v] err[%v]", msgH.inConn.RemoteAddr().String(), pkg.nextAddr, err.Error())
		err = errors.Annotatef(fmt.Errorf(msg), "Request[%v] sendToNext Error", pkg.GetUniqLogId())
		pkg.PackErrorBody(ActionSendToNext, err.Error())
	}

	return err
}

func (s *DataNode) CheckStoreMode(p *Packet) (err error) {
	if p.StoreMode == proto.TinyStoreMode || p.StoreMode == proto.ExtentStoreMode {
		return nil
	}
	return ErrStoreTypeMismatch
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
	pkg.dataPartition = s.space.getDataPartition(pkg.PartitionID)
	if pkg.dataPartition == nil {
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
	stat := s.space.stats
	if pkg == nil {
		return
	}
	if flag == OutFlow {
		stat.AddInDataSize(uint64(pkg.Size + pkg.Arglen))
		return
	}

	if pkg.IsReadReq() {
		stat.AddInDataSize(uint64(pkg.Arglen))
	} else {
		stat.AddInDataSize(uint64(pkg.Size + pkg.Arglen))
	}

}
