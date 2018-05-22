package proto

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
	"sync/atomic"
	"time"
)

var (
	ReqIDGlobal = int64(1)
)

func GetReqID() int64 {
	return atomic.AddInt64(&ReqIDGlobal, 1)
}

const (
	ExtentNameSplit = "_"
	VolNameSplit    = "_"
	AddrSplit       = "/"
	HeaderSize      = 45
	PkgArgMaxSize   = 100
	ConnBufferSize  = 4096
)

//operations
const (
	ProtoMagic        uint8 = 0xFF
	OpCreateFile      uint8 = 0x01
	OpMarkDelete      uint8 = 0x02
	OpWrite           uint8 = 0x03
	OpRead            uint8 = 0x04
	OpStreamRead      uint8 = 0x05
	OpGetWatermark    uint8 = 0x06
	OpGetAllWatermark uint8 = 0x07
	OpNotifyRepair    uint8 = 0x08
	OpERepairRead     uint8 = 0x09
	OpCRepairRead     uint8 = 0x0A
	OpFlowInfo        uint8 = 0x0B
	OpSyncDelNeedle   uint8 = 0x0C
	OpNotifyCompact   uint8 = 0x0D

	// Operations: Client -> MetaNode.
	OpMetaCreateInode  uint8 = 0x0E
	OpMetaDeleteInode  uint8 = 0x0F
	OpMetaCreateDentry uint8 = 0x10
	OpMetaDeleteDentry uint8 = 0x11
	OpMetaOpen         uint8 = 0x13
	OpMetaLookup       uint8 = 0x14
	OpMetaReadDir      uint8 = 0x15
	OpMetaInodeGet     uint8 = 0x16
	OpMetaExtentsAdd   uint8 = 0x17
	OpMetaExtentsDel   uint8 = 0x18
	OpMetaExtentsList  uint8 = 0x19

	// Operations: Master -> MetaNode
	OpCreateMetaPartition  uint8 = 0x20
	OpMetaNodeHeartbeat    uint8 = 0x21
	OpDeleteMetaPartition  uint8 = 0x22
	OpUpdateMetaPartition  uint8 = 0x23
	OpLoadMetaPartition    uint8 = 0x24
	OpOfflineMetaPartition uint8 = 0x25

	OpCreateVol         uint8 = 0x26
	OpDeleteVol         uint8 = 0x27
	OpLoadVol           uint8 = 0x28
	OpDataNodeHeartbeat uint8 = 0x29

	// Commons
	OpIntraGroupNetErr uint8 = 0xF3
	OpArgMismatchErr   uint8 = 0xF4
	OpNotExistErr      uint8 = 0xF5
	OpDiskNoSpaceErr   uint8 = 0xF6
	OpDiskErr          uint8 = 0xF7
	OpErr              uint8 = 0xF8
	OpAgain            uint8 = 0xF9
	OpExistErr         uint8 = 0xFA
	OpInodeFullErr     uint8 = 0xFB
	OpArgUnmatchErr    uint8 = 0xFC
	OpOk               uint8 = 0xF0
)

const (
	WriteDeadlineTime  = 5
	ReadDeadlineTime   = 3
	NoReadDeadlineTime = -1
)

const (
	TinyStoreMode   = 0
	ExtentStoreMode = 1
)

type Packet struct {
	Magic      uint8
	StoreMode  uint8
	Opcode     uint8
	ResultCode uint8
	Nodes      uint8
	Crc        uint32
	Size       uint32
	Arglen     uint32
	VolID      uint32
	FileID     uint64
	Offset     int64
	ReqID      int64
	Arg        []byte //if create or append ops, data contains addrs
	Data       []byte
	StartT     int64
}

func NewPacket() *Packet {
	p := new(Packet)
	p.Magic = ProtoMagic
	p.StartT = time.Now().UnixNano()

	return p
}

func (p *Packet) GetOpMesg(opcode uint8) (m string) {
	switch opcode {
	case OpCreateFile:
		m = "CreateFile"
	case OpMarkDelete:
		m = "MarkDelete"
	case OpWrite:
		m = "Write"
	case OpRead:
		m = "Read"
	case OpStreamRead:
		m = "StreamRead"
	case OpGetWatermark:
		m = "GetWatermark"
	case OpGetAllWatermark:
		m = "GetAllWatermark"
	case OpNotifyRepair:
		m = "NotifyRepair"
	case OpCRepairRead:
		m = "ChunkRepairRead"
	case OpNotifyCompact:
		m = "NotifyCompact"
	case OpERepairRead:
		m = "ExtentRepairRead"
	case OpFlowInfo:
		m = "FlowInfo"
	case OpIntraGroupNetErr:
		m = "IntraGroupNetErr"
	case OpArgMismatchErr:
		m = "ArgUnmatchErr"
	case OpNotExistErr:
		m = "NotExistErr"
	case OpDiskNoSpaceErr:
		m = "DiskNoSpaceErr"
	case OpDiskErr:
		m = "DiskErr"
	case OpErr:
		m = "Err"
	case OpAgain:
		m = "Again"
	case OpOk:
		m = "Ok"
	case OpSyncDelNeedle:
		m = "OpSyncHasDelNeedle"
	case OpExistErr:
		m = "ExistErr"
	case OpInodeFullErr:
		m = "InodeFullErr"
	default:
		return ""

	}
	return
}

func (p *Packet) MarshalHeader(out []byte) {
	out[0] = p.Magic
	out[1] = p.StoreMode
	out[2] = p.Opcode
	out[3] = p.ResultCode
	out[4] = p.Nodes
	binary.BigEndian.PutUint32(out[5:9], p.Crc)
	binary.BigEndian.PutUint32(out[9:13], p.Size)
	binary.BigEndian.PutUint32(out[13:17], p.Arglen)
	binary.BigEndian.PutUint32(out[17:21], p.VolID)
	binary.BigEndian.PutUint64(out[21:29], p.FileID)
	binary.BigEndian.PutUint64(out[29:37], uint64(p.Offset))
	binary.BigEndian.PutUint64(out[37:HeaderSize], uint64(p.ReqID))

	return
}

func (p *Packet) UnmarshalHeader(in []byte) error {
	p.Magic = in[0]
	if p.Magic != ProtoMagic {
		return errors.New("Bad Magic " + strconv.Itoa(int(p.Magic)))
	}

	p.StoreMode = in[1]
	p.Opcode = in[2]
	p.ResultCode = in[3]
	p.Nodes = in[4]
	p.Crc = binary.BigEndian.Uint32(in[5:9])
	p.Size = binary.BigEndian.Uint32(in[9:13])
	p.Arglen = binary.BigEndian.Uint32(in[13:17])
	p.VolID = binary.BigEndian.Uint32(in[17:21])
	p.FileID = binary.BigEndian.Uint64(in[21:29])
	p.Offset = int64(binary.BigEndian.Uint64(in[29:37]))
	p.ReqID = int64(binary.BigEndian.Uint64(in[37:HeaderSize]))

	return nil
}

func (p *Packet) MarshalData(v interface{}) error {
	data, err := json.Marshal(v)
	if err == nil {
		p.Data = data
		p.Size = uint32(len(p.Data))
	}
	return err
}

func (p *Packet) UnmarshalData(v interface{}) error {
	return json.Unmarshal(p.Data, v)
}

func (p *Packet) WriteToNoDeadLineConn(c net.Conn) (err error) {
	header := make([]byte, HeaderSize)

	p.MarshalHeader(header)
	if _, err = c.Write(header); err == nil {
		if _, err = c.Write(p.Arg[:int(p.Arglen)]); err == nil {
			if p.Data != nil {
				_, err = c.Write(p.Data[:p.Size])
			}
		}
	}

	return
}

func (p *Packet) WriteToConn(c net.Conn) (err error) {
	c.SetWriteDeadline(time.Now().Add(WriteDeadlineTime * time.Second))
	header := make([]byte, HeaderSize)

	p.MarshalHeader(header)
	if _, err = c.Write(header); err == nil {
		if _, err = c.Write(p.Arg[:int(p.Arglen)]); err == nil {
			if p.Data != nil {
				_, err = c.Write(p.Data[:p.Size])
			}
		}
	}

	return
}

func (p *Packet) WriteHeaderToConn(c net.Conn) (err error) {
	header := make([]byte, HeaderSize)
	p.MarshalHeader(header)
	_, err = c.Write(header)

	return
}

func ReadFull(c net.Conn, buf *[]byte, readSize int) (err error) {
	*buf = make([]byte, readSize)
	_, err = io.ReadFull(c, (*buf)[:readSize])

	return
}

func (p *Packet) ReadFromConn(c net.Conn, deadlineTime time.Duration) (err error) {
	if deadlineTime != NoReadDeadlineTime {
		c.SetReadDeadline(time.Now().Add(deadlineTime * time.Second))
	}
	header := make([]byte, HeaderSize)
	if _, err = io.ReadFull(c, header); err != nil {
		return
	}
	if err = p.UnmarshalHeader(header); err != nil {
		return
	}

	if p.Arglen > 0 {
		if err = ReadFull(c, &p.Arg, int(p.Arglen)); err != nil {
			return
		}
	}

	if p.Size < 0 {
		return
	}
	size := p.Size
	if p.Opcode == OpRead || p.Opcode == OpStreamRead {
		size = 0
	}
	return ReadFull(c, &p.Data, int(size))
}

func (p *Packet) PackOkReply() {
	p.ResultCode = OpOk
	p.Size = 0
	p.Arglen = 0
}

func (p *Packet) PackOkReadReply() {
	p.ResultCode = OpOk
	p.Arglen = 0
}

func (p *Packet) PackOkGetWatermarkReply(size int64) {
	p.Offset = size
	p.Size = 0
	p.ResultCode = OpOk
	p.Arglen = 0
}

func (p *Packet) IsOkReply() bool {
	return p.ResultCode == OpOk
}

func (p *Packet) PackOkWithBody(reply []byte) {
	p.Size = uint32(len(reply))
	p.Data = make([]byte, p.Size)
	copy(p.Data[:p.Size], reply)
	p.ResultCode = OpOk
	p.Arglen = 0
}

func (p *Packet) PackErrorWithBody(errCode uint8, reply []byte) {
	p.Size = uint32(len(reply))
	p.Data = make([]byte, p.Size)
	copy(p.Data[:p.Size], reply)
	p.ResultCode = errCode
	p.Arglen = 0
}

func (p *Packet) GetUniqLogId() (m string) {
	m = fmt.Sprintf("%v_%v_%v_%v_%v_%v_%v", p.ReqID, p.VolID, p.FileID,
		p.Offset, p.Size, p.GetOpMesg(p.Opcode), p.GetOpMesg(p.ResultCode))

	return
}

func (p *Packet) IsTransitPkg() bool {
	return p.Nodes > 0
}

func (p *Packet) ActionMesg(action, remote string, start int64, err error) (m string) {
	if err == nil {
		m = fmt.Sprintf("id[%v] act[%v] remote[%v] "+
			" cost[%v] isTransite[%v] ",
			p.GetUniqLogId(), action, remote,
			(time.Now().UnixNano()-start)/1e6, p.IsTransitPkg())

	} else {
		m = fmt.Sprintf("id[%v] act[%v] remote[%v]"+
			", err[%v] isTransite[%v]", p.GetUniqLogId(), action,
			remote, err.Error(), p.IsTransitPkg())
	}

	return
}
