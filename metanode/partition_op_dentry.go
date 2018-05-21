package metanode

import (
	"encoding/json"

	"github.com/tiglabs/baudstorage/proto"
)

func (mp *metaPartition) CreateDentry(req *CreateDentryReq, p *Packet) (err error) {
	dentry := &Dentry{
		ParentId: req.ParentID,
		Name:     req.Name,
		Inode:    req.Inode,
		Type:     req.Mode,
	}
	val, err := json.Marshal(dentry)
	if err != nil {
		return
	}
	resp, err := mp.Put(opCreateDentry, val)
	if err != nil {
		return
	}
	p.ResultCode = resp.(uint8)
	return
}

func (mp *metaPartition) DeleteDentry(req *DeleteDentryReq, p *Packet) (err error) {
	var resp *DeleteDentryResp
	dentry := &Dentry{
		ParentId: req.ParentID,
		Name:     req.Name,
	}
	val, err := json.Marshal(dentry)
	if err != nil {
		p.ResultCode = proto.OpErr
		return
	}
	r, err := mp.Put(opDeleteDentry, val)
	if err != nil {
		p.ResultCode = proto.OpErr
		return
	}
	p.ResultCode = r.(uint8)
	if p.ResultCode == proto.OpOk {
		var reply []byte
		resp.Inode = dentry.Inode
		reply, err = json.Marshal(resp)
		p.PackOkWithBody(reply)
	}
	return
}

func (mp *metaPartition) ReadDir(req *ReadDirReq, p *Packet) (err error) {
	resp := mp.readDir(req)
	reply, err := json.Marshal(resp)
	if err != nil {
		p.PackErrorWithBody(proto.OpErr, nil)
		return
	}
	p.PackOkWithBody(reply)
	return
}

func (mp *metaPartition) Lookup(req *LookupReq, p *Packet) (err error) {
	if err != nil {
		p.PackErrorWithBody(proto.OpErr, nil)
		return
	}
	dentry := &Dentry{
		ParentId: req.PartitionID,
		Name:     req.Name,
	}
	status := mp.getDentry(dentry)
	p.PackErrorWithBody(status, nil)
	return
}
