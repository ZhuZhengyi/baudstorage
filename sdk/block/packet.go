package block

import (
	"errors"
	"fmt"
	"hash/crc32"
	"strconv"
	"strings"
	"sync/atomic"

	"github.com/tiglabs/baudstorage/proto"
	"github.com/tiglabs/baudstorage/sdk"
)

const KeySegsCnt = 7 // cfs/{version}/{vid}/{fid}/{offset}/{size}/{crc}
var reqId int64 = 0

func allocReqId() int64 {
	atomic.AddInt64(&reqId, 1)
	return reqId
}

func newWritePacket(data []byte, vol *sdk.VolGroup) (pkg *proto.Packet) {
	pkg = proto.NewPacket()
	pkg.Opcode = proto.OpWrite
	pkg.StoreMode = proto.TinyStoreMode
	pkg.Nodes = 1
	pkg.Size = uint32(len(data))
	pkg.Data = data
	pkg.Crc = crc32.ChecksumIEEE(data)
	argstr := vol.Hosts[1] + proto.AddrSplit + vol.Hosts[2]
	pkg.Arg = []byte(argstr)
	pkg.Arglen = uint32(len(pkg.Arg))
	pkg.VolID = vol.VolID
	pkg.ReqID = allocReqId()
	return
}

func newReadPacket(vid, size uint32, fid uint64, ofs int64) (pkg *proto.Packet) {
	pkg = proto.NewPacket()
	pkg.Opcode = proto.OpRead
	pkg.StoreMode = proto.TinyStoreMode
	pkg.Nodes = 0
	pkg.Size = size
	pkg.FileID = fid
	pkg.Offset = ofs
	pkg.Arglen = 0
	pkg.VolID = vid
	pkg.ReqID = allocReqId()
	return
}

func newDelPacket(fid uint64, size uint32, ofs int64, vol *sdk.VolGroup) (pkg *proto.Packet) {
	pkg = proto.NewPacket()
	pkg.Opcode = proto.OpMarkDelete
	pkg.StoreMode = proto.TinyStoreMode
	pkg.Nodes = 1
	pkg.Size = size
	argstr := vol.Hosts[1] + proto.AddrSplit + vol.Hosts[2]
	pkg.Arg = []byte(argstr)
	pkg.Arglen = uint32(len(pkg.Arg))
	pkg.VolID = vol.VolID
	pkg.Offset = ofs
	pkg.FileID = fid
	pkg.ReqID = allocReqId()
	return
}

func marshalKey(pkt *proto.Packet) string {
	key := fmt.Sprintf("cfs/%v/%x/%x/%x/%x/%x", Version, pkt.VolID, pkt.FileID, pkt.Offset, pkt.Size, pkt.Crc)
	return key
}

func unmarshalKey(key string) (volId uint32, fileId uint64, offset int64, size uint32, crc uint32, err error) {
	var tmp uint64
	segs := strings.Split(key, "/")
	if len(segs) != KeySegsCnt || segs[0] != "cfs" || segs[1] != Version {
		goto Failed
	}
	tmp, err = strconv.ParseUint(segs[2], 16, 32)
	if err != nil {
		goto Failed
	}
	volId = uint32(tmp)

	fileId, err = strconv.ParseUint(segs[3], 16, 64)
	if err != nil {
		goto Failed
	}

	offset, err = strconv.ParseInt(segs[4], 16, 64)
	if err != nil {
		goto Failed
	}

	tmp, err = strconv.ParseUint(segs[5], 16, 32)
	if err != nil {
		goto Failed
	}
	size = uint32(tmp)

	tmp, err = strconv.ParseUint(segs[6], 16, 32)
	if err != nil {
		goto Failed
	}
	crc = uint32(tmp)
	return
Failed:
	err = errors.New("invalid key ")
	return
}
