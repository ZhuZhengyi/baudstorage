package datanode

import (
	"errors"
	"fmt"
	"github.com/tiglabs/baudstorage/proto"
	"github.com/tiglabs/baudstorage/storage"
	"github.com/tiglabs/baudstorage/util/ump"
	"net"
	"strings"
	"time"
)

var (
	ErrBadNodes          = errors.New("BadNodesErr")
	ErrArgLenUnmatch     = errors.New("ArgLenMismatchErr")
	ErrAddrsNodesUnmatch = errors.New("AddrsNodesMismatchErr")
)

type Packet struct {
	proto.Packet
	goals    uint8
	nextConn net.Conn
	nextAddr string
	addrs    []string
	isReturn bool
	vol      *Vol
	tpObject *ump.TpObject
}

func (p *Packet) afterTp() (ok bool) {
	var err error
	if p.IsErrPack() {
		err = fmt.Errorf(p.GetOpMsg()+" failed because[%v]", string(p.Data[:p.Size]))
	}
	ump.AfterTP(p.tpObject, err)

	return
}

func (p *Packet) beforeTp(clusterId string) (ok bool) {
	umpKey := fmt.Sprintf("%s_datanode_stream%v", clusterId, p.GetOpMsg())
	p.tpObject = ump.BeforeTP(umpKey)
	return
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

func (p *Packet) IsMasterCommand() bool {
	if p.Opcode == proto.OpDataNodeHeartbeat || p.Opcode == proto.OpLoadVol || p.Opcode == proto.OpCreateVol {
		return true
	}
	return false
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

func (p *Packet) CopyFrom(src *Packet) {
	p.ResultCode = src.ResultCode
	p.Opcode = src.Opcode
	p.Size = src.Size
	p.Data = src.Data
}

func (p *Packet) IsErrPack() bool {
	return p.ResultCode != proto.OpOk
}

func (p *Packet) getErr() (m string) {
	return fmt.Sprintf("req[%v] err[%v]", p.GetUniqLogId(), string(p.Data[:p.Size]))
}

func (p *Packet) ClassifyErrorOp(errLog string, errMsg string) {
	if strings.Contains(errLog, ActionReciveFromNext) || strings.Contains(errLog, ActionSendToNext) ||
		strings.Contains(errLog, ConnIsNullErr) || strings.Contains(errLog, ActioncheckAndAddInfos) {
		p.ResultCode = proto.OpIntraGroupNetErr
		return
	}

	if strings.Contains(errMsg, storage.ErrorUnmatchPara.Error()) ||
		strings.Contains(errMsg, ErrorUnknowOp.Error()) {
		p.ResultCode = proto.OpArgUnmatchErr
	} else if strings.Contains(errMsg, storage.ErrorObjNotFound.Error()) ||
		strings.Contains(errMsg, storage.ErrorHasDelete.Error()) {
		p.ResultCode = proto.OpNotExistErr
	} else if strings.Contains(errMsg, storage.ErrSyscallNoSpace.Error()) {
		p.ResultCode = proto.OpDiskNoSpaceErr
	} else if strings.Contains(errMsg, storage.ErrorAgain.Error()) {
		p.ResultCode = proto.OpIntraGroupNetErr
	} else if strings.Contains(errMsg, storage.ErrorChunkNotFound.Error()) {
		if p.Opcode != proto.OpWrite {
			p.ResultCode = proto.OpNotExistErr
		} else {
			p.ResultCode = proto.OpIntraGroupNetErr
		}
	} else {
		p.ResultCode = proto.OpIntraGroupNetErr
	}
}

func (p *Packet) PackErrorBody(action, msg string) {
	p.ClassifyErrorOp(action, msg)
	if p.ResultCode == proto.OpDiskNoSpaceErr || p.ResultCode == proto.OpDiskErr {
		p.ResultCode = proto.OpIntraGroupNetErr
	}
	p.Size = uint32(len([]byte(action + "_" + msg)))
	p.Data = make([]byte, p.Size)
	copy(p.Data[:int(p.Size)], []byte(action+"_"+msg))
}
