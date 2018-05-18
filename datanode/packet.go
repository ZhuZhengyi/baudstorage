package datanode

import (
	"errors"
	"fmt"
	"github.com/tiglabs/baudstorage/proto"
	"github.com/tiglabs/baudstorage/storage"
	"net"
	"strings"
	"time"
)

var (
	ErrBadNodes          = errors.New("BadNodesErr")
	ErrArgLenUnmatch     = errors.New("ArgLenUnmatchErr")
	ErrAddrsNodesUnmatch = errors.New("AddrsNodesUnmatchErr")
)

type Packet struct {
	proto.Packet
	goals    uint8
	nextConn net.Conn
	nextAddr string
	addrs    []string
	isReturn bool
	vol      *Vol
}

func (p *Packet) UnmarshalAddrs() (addrs []string, err error) {
	if len(p.Arg) < int(p.Arglen) {
		return nil, ErrArgLenUnmatch
	}
	str := string(p.Arg[:int(p.Arglen)])
	goalAddrs := strings.SplitN(str, proto.AddrSplit, -1)
	p.goals = uint8(len(goalAddrs) - 1)
	if p.goals > 0 {
		addrs = goalAddrs[:int(p.goals)]
	}
	if p.Nodes < 0 {
		err = ErrBadNodes
		return
	}
	copy(p.addrs, addrs)

	return
}

func NewPacket() (p *Packet) {
	p = new(Packet)
	p.Magic = proto.ProtoMagic
	p.StartT = time.Now().UnixNano()
	return
}

func (p *Packet) GetNextAddr(addrs []string) error {
	sub := p.goals - p.Nodes
	if sub < 0 || sub > p.goals || (sub == p.goals && p.Nodes != 0) {
		return ErrAddrsNodesUnmatch
	}
	if sub == p.goals && p.Nodes == 0 {
		return nil
	}

	p.nextAddr = fmt.Sprint(addrs[sub])

	return nil
}

func (p *Packet) IsTransitPkg() bool {
	r := p.Nodes > 0
	return r
}

func NewGetAllWaterMarker(volId uint32, storeMode uint8) (p *Packet) {
	p = new(Packet)
	p.Opcode = proto.OpGetAllWatermark
	p.VolID = volId
	p.Magic = proto.ProtoMagic
	p.ReqID = proto.GetReqID()
	p.StoreMode = storeMode

	return
}

func NewStreamReadPacket(volId uint32, extentId, offset, size int) (p *Packet) {
	p = new(Packet)
	p.FileID = uint64(extentId)
	p.VolID = volId
	p.Magic = proto.ProtoMagic
	p.Offset = int64(offset)
	p.Size = uint32(size)
	p.Opcode = proto.OpStreamRead
	p.StoreMode = proto.ExtentStoreMode
	p.ReqID = proto.GetReqID()

	return
}

func NewStreamChunkRepairReadPacket(volId uint32, chunkId int) (p *Packet) {
	p = new(Packet)
	p.FileID = uint64(chunkId)
	p.VolID = volId
	p.Magic = proto.ProtoMagic
	p.Opcode = proto.OpCRepairRead
	p.StoreMode = proto.TinyStoreMode
	p.ReqID = proto.GetReqID()

	return
}

func NewNotifyRepair(volId uint32, storeMode int) (p *Packet) {
	p = new(Packet)
	p.Opcode = proto.OpNotifyRepair
	p.VolID = volId
	p.Magic = proto.ProtoMagic
	p.ReqID = proto.GetReqID()
	p.StoreMode = uint8(storeMode)

	return
}

func (p *Packet) IsTailNode() (ok bool) {
	if p.Nodes == 0 && (p.IsWriteOperation() || p.Opcode == proto.OpCreateFile ||
		(p.Opcode == proto.OpMarkDelete && p.StoreMode == proto.TinyStoreMode)) {
		return true
	}

	return
}

func (p *Packet) IsWriteOperation() bool {
	return p.Opcode == proto.OpWrite
}

func (p *Packet) IsReadReq() bool {
	return p.Opcode == proto.OpStreamRead || p.Opcode == proto.OpRead
}

func (p *Packet) IsMarkDeleteReq() bool {
	return p.Opcode == proto.OpMarkDelete
}

func (p *Packet) isHeadNode() (ok bool) {
	if p.goals == p.Nodes && (p.IsWriteOperation() || p.Opcode == proto.OpCreateFile ||
		(p.Opcode == proto.OpMarkDelete)) {
		ok = true
	}

	return
}

func (p *Packet) IsErrPack() bool {
	return p.ResultCode == proto.OpOk
}

func (p *Packet) getErr() (m string) {
	return fmt.Sprintf("req[%v] err[%v]", p.GetUniqLogId(), string(p.Data[:p.Size]))
}

func (p *Packet) actionMesg(action, remote string, start int64, err error) (m string) {
	if err == nil {
		m = fmt.Sprintf("id[%v] act[%v] remote[%v] op[%v] local[%v] size[%v] "+
			" cost[%v] isTransite[%v] ",
			p.GetUniqLogId(), action, remote, proto.GetOpMesg(p.Opcode), proto.GetOpMesg(p.ResultCode), p.Size,
			(time.Now().UnixNano()-start)/1e6, p.IsTransitPkg())

	} else {
		m = fmt.Sprintf("id[%v] act[%v] remote[%v] op[%v] local[%v] size[%v] "+
			", err[%v] isTransite[%v]", p.GetUniqLogId(), action,
			remote, proto.GetOpMesg(p.Opcode), proto.GetOpMesg(p.ResultCode), p.Size, err.Error(),
			p.IsTransitPkg())
	}

	return
}

func (p *Packet) ClassifyErrorOp(errLog string, errMsg string) {
	if strings.Contains(errLog, ActionReciveFromNext) || strings.Contains(errLog, ActionSendToNext) ||
		strings.Contains(errLog, ConnIsNullErr) || strings.Contains(errLog, ActioncheckAndAddInfos) {
		p.Opcode = proto.OpIntraGroupNetErr
		return
	}

	if strings.Contains(errMsg, storage.ErrorUnmatchPara.Error()) ||
		strings.Contains(errMsg, ErrorUnknowOp.Error()) {
		p.Opcode = proto.OpArgUnmatchErr
	} else if strings.Contains(errMsg, storage.ErrorObjNotFound.Error()) ||
		strings.Contains(errMsg, storage.ErrorHasDelete.Error()) {
		p.Opcode = proto.OpNotExistErr
	} else if strings.Contains(errMsg, storage.ErrSyscallNoSpace.Error()) {
		p.Opcode = proto.OpDiskNoSpaceErr
	} else if strings.Contains(errMsg, storage.ErrorAgain.Error()) {
		p.Opcode = proto.OpIntraGroupNetErr
	} else if strings.Contains(errMsg, storage.ErrorChunkNotFound.Error()) {
		if p.Opcode != proto.OpWrite {
			p.Opcode = proto.OpNotExistErr
		} else {
			p.Opcode = proto.OpIntraGroupNetErr
		}
	} else {
		p.Opcode = proto.OpIntraGroupNetErr
	}
}

func (p *Packet) PackErrorBody(action, msg string) {
	p.ClassifyErrorOp(action, msg)
	if p.Opcode == proto.OpDiskNoSpaceErr || p.Opcode == proto.OpDiskErr {
		p.Opcode = proto.OpIntraGroupNetErr
	}
	p.Size = uint32(len([]byte(action + "_" + msg)))
	p.Data = make([]byte, p.Size)
	copy(p.Data[:int(p.Size)], []byte(action+"_"+msg))
}
