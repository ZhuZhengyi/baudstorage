package stream

import (
	"hash/crc32"
	"net"

	"github.com/tiglabs/baudstorage/proto"
	"github.com/tiglabs/baudstorage/sdk"
	"github.com/tiglabs/baudstorage/util"
)

type Packet struct {
	proto.Packet
	fillBytes uint32
}

func NewPacket(vol *sdk.VolGroup) (p *Packet) {
	p = new(Packet)
	p.VolID = vol.VolId
	p.Magic = proto.ProtoMagic
	p.Data = make([]byte, 0)
	p.Opcode = proto.OpWrite
	p.Size = 0
	p.StoreType = proto.ExtentStoreMode
	p.ReqID = proto.GetReqID()

	return
}

func NewWritePacket(vol *sdk.VolGroup, extentId, seqNo uint64, offset int) (p *Packet) {
	p = NewPacket(vol)
	p.FileID = extentId
	p.Offset = int64(offset)
	p.Arg = ([]byte)(vol.GetAllAddrs())
	p.Arglen = uint32(len(p.Arg))
	p.ReqID = int64(seqNo)

	return
}

func NewReply(reqId int64, volId uint32, extentId uint64) (p *Packet) {
	p = new(Packet)
	p.ReqID = reqId
	p.VolID = volId
	p.FileID = extentId

	return
}

func IsEqual(request, reply *Packet) bool {
	if request.ReqID == reply.ReqID && request.VolID == reply.VolID && request.FileID == reply.FileID {
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

func (p *Packet) isFull() bool {
	return len(p.Data)-CFSBLOCKSIZE == 0
}

func (p *Packet) getDataLength() int {
	return len(p.Data)
}

func (p *Packet) writeTo(conn net.Conn) (err error) {
	p.Crc = crc32.ChecksumIEEE(p.Data)
	err = p.WriteToConn(conn)

	return
}
