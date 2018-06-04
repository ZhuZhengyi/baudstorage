package stream

import (
	"hash/crc32"
	"net"

	"github.com/tiglabs/baudstorage/proto"
	"github.com/tiglabs/baudstorage/sdk/data"
	"github.com/tiglabs/baudstorage/util"
)

type Packet struct {
	proto.Packet
	fillBytes uint32
}

func NewWritePacket(dp *data.DataPartion, extentId, seqNo uint64, offset int) (p *Packet) {
	p = new(Packet)
	p.PartionID = dp.DataPartionID
	p.Magic = proto.ProtoMagic
	p.Data = make([]byte, 0)
	p.StoreMode = proto.ExtentStoreMode
	p.FileID = extentId
	p.Offset = int64(offset)
	p.Arg = ([]byte)(dp.GetAllAddrs())
	p.Arglen = uint32(len(p.Arg))
	p.Nodes = uint8(len(dp.Hosts) - 1)
	p.ReqID = proto.GetReqID()
	p.Opcode = proto.OpWrite

	return
}

func NewReadPacket(key proto.ExtentKey, offset, size int) (p *Packet) {
	p = new(Packet)
	p.FileID = key.ExtentId
	p.PartionID = key.PartionId
	p.Magic = proto.ProtoMagic
	p.Offset = int64(offset)
	p.Size = uint32(size)
	p.Opcode = proto.OpStreamRead
	p.StoreMode = proto.ExtentStoreMode
	p.ReqID = proto.GetReqID()
	p.Nodes = 0

	return
}

func NewCreateExtentPacket(dp *data.DataPartion) (p *Packet) {
	p = new(Packet)
	p.PartionID = dp.DataPartionID
	p.Magic = proto.ProtoMagic
	p.Data = make([]byte, 0)
	p.StoreMode = proto.ExtentStoreMode
	p.Arg = ([]byte)(dp.GetAllAddrs())
	p.Arglen = uint32(len(p.Arg))
	p.Nodes = uint8(len(dp.Hosts) - 1)
	p.ReqID = proto.GetReqID()
	p.Opcode = proto.OpCreateFile

	return p
}

func NewDeleteExtentPacket(dp *data.DataPartion, extentId uint64) (p *Packet) {
	p = new(Packet)
	p.Magic = proto.ProtoMagic
	p.Opcode = proto.OpMarkDelete
	p.StoreMode = proto.ExtentStoreMode
	p.PartionID = dp.DataPartionID
	p.FileID = extentId
	p.ReqID = proto.GetReqID()
	p.Nodes = uint8(len(dp.Hosts) - 1)
	p.Arg = ([]byte)(dp.GetAllAddrs())
	p.Arglen = uint32(len(p.Arg))
	return p
}

func NewReply(reqId int64, volID uint32, extentId uint64) (p *Packet) {
	p = new(Packet)
	p.ReqID = reqId
	p.PartionID = volID
	p.FileID = extentId
	p.Magic = proto.ProtoMagic
	p.StoreMode = proto.ExtentStoreMode

	return
}

func (p *Packet) IsEqual(q *Packet) bool {
	if p.ReqID == q.ReqID && p.PartionID == q.PartionID && p.FileID == q.FileID {
		return true
	}

	return false
}

func (p *Packet) fill(data []byte, size int) (canWrite int) {
	blockSpace := CFSBLOCKSIZE - (p.Offset % CFSBLOCKSIZE)
	remain := int(blockSpace) - int(p.Size)
	canWrite = util.Min(remain, size)
	p.Data = append(p.Data, data[:canWrite]...)
	p.Size = uint32(len(p.Data))

	return
}

func (p *Packet) isFullPacket() bool {
	return p.Size-CFSBLOCKSIZE == 0
}

func (p *Packet) getPacketLength() int {
	return len(p.Data)
}

func (p *Packet) writeTo(conn net.Conn) (err error) {
	p.Crc = crc32.ChecksumIEEE(p.Data)
	err = p.WriteToConn(conn)

	return
}
