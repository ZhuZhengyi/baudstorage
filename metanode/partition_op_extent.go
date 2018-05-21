package metanode

import (
	"encoding/json"
	"github.com/tiglabs/baudstorage/proto"
)

func (mp *metaPartition) ExtentAppend(req *proto.AppendExtentKeyRequest, p *Packet) (err error) {
	ino := NewInode(req.Inode, 0)
	ino.Extents = req.Extents
	val, err := json.Marshal(ino)
	if err != nil {
		p.PackErrorWithBody(proto.OpErr, nil)
		return
	}
	resp, err := mp.Put(opExtentsAdd, val)
	if err != nil {
		p.PackErrorWithBody(proto.OpErr, nil)
		return
	}
	p.PackErrorWithBody(resp.(uint8), nil)
	return
}

func (mp *metaPartition) ExtentsList(req *proto.GetExtentsRequest,
	p *Packet) (err error) {
	ino := NewInode(req.Inode, 0)
	status := mp.getInode(ino)
	var reply []byte
	if status == proto.OpOk {
		resp := &proto.GetExtentsResponse{
			Extents: ino.Extents,
		}
		reply, err = json.Marshal(resp)
		if err != nil {
			status = proto.OpErr
		}
	}
	p.PackErrorWithBody(status, reply)
	return
}