package metanode

import (
	"encoding/json"
	"github.com/tiglabs/baudstorage/proto"
	"time"
)

func (mp *metaPartition) CreateInode(req *CreateInoReq, p *Packet) (err error) {
	inoID, err := mp.nextInodeID()
	if err != nil {
		err = nil
		p.PackErrorWithBody(proto.OpInodeFullErr, nil)
		return
	}
	ino := NewInode(inoID, req.Mode)
	val, err := json.Marshal(ino)
	if err != nil {
		p.PackErrorWithBody(proto.OpErr, nil)
		return
	}
	r, err := mp.Put(opCreateInode, val)
	if err != nil {
		p.PackErrorWithBody(proto.OpErr, nil)
		return
	}
	var (
		status = r.(uint8)
		reply  []byte
	)
	if status == proto.OpOk {
		resp := &CreateInoResp{}
		resp.Info.Inode = ino.Inode
		resp.Info.Mode = ino.Type
		resp.Info.Size = ino.Size
		resp.Info.CreateTime = time.Unix(ino.CreateTime, 0)
		resp.Info.ModifyTime = time.Unix(ino.ModifyTime, 0)
		resp.Info.AccessTime = time.Unix(ino.AccessTime, 0)
		reply, err = json.Marshal(resp)
		if err != nil {
			status = proto.OpErr
		}
	}
	p.PackErrorWithBody(status, reply)
	return
}

func (mp *metaPartition) DeleteInode(req *DeleteInoReq, p *Packet) (err error) {
	ino := NewInode(req.Inode, 0)
	val, err := json.Marshal(ino)
	if err != nil {
		p.PackErrorWithBody(proto.OpErr, nil)
		return
	}
	r, err := mp.Put(opDeleteInode, val)
	if err != nil {
		p.PackErrorWithBody(proto.OpErr, nil)
		return
	}
	p.PackErrorWithBody(r.(uint8), nil)
	return
}

func (mp *metaPartition) Open(req *OpenReq, p *Packet) (err error) {
	ino := NewInode(req.Inode, 0)
	val, err := json.Marshal(ino)
	if err != nil {
		p.PackErrorWithBody(proto.OpErr, nil)
		return
	}
	resp, err := mp.Put(opOpen, val)
	p.PackErrorWithBody(resp.(uint8), nil)
	return
}

func (mp *metaPartition) InodeGet(req *proto.InodeGetRequest,
	p *Packet) (err error) {
	ino := NewInode(req.Inode, 0)
	if err != nil {
		p.PackErrorWithBody(proto.OpErr, nil)
		return
	}
	status := mp.getInode(ino)
	var reply []byte
	if status == proto.OpOk {
		resp := &proto.InodeGetResponse{
			Info: &proto.InodeInfo{},
		}
		resp.Info.Inode = ino.Inode
		resp.Info.Mode = ino.Type
		resp.Info.Size = ino.Size
		resp.Info.CreateTime = time.Unix(ino.CreateTime, 0)
		resp.Info.AccessTime = time.Unix(ino.AccessTime, 0)
		resp.Info.ModifyTime = time.Unix(ino.ModifyTime, 0)
		reply, err = json.Marshal(resp)
		if err != nil {
			status = proto.OpErr
		}
	}
	p.PackErrorWithBody(status, reply)
	return
}

func (mp *metaPartition) DeletePartition() (err error) {
	_, err = mp.Put(opDeletePartition, nil)
	return
}

func (mp *metaPartition) UpdatePartition(req *proto.UpdateMetaPartitionRequest) (err error) {
	reqData, err := json.Marshal(req)
	if err != nil {
		return
	}
	_, err = mp.Put(opUpdatePartition, reqData)
	return
}

func (mp *metaPartition) OfflinePartition(req []byte) (err error) {
	_, err = mp.Put(opOfflinePartition, req)
	return
}