package metanode

import (
	"encoding/json"
	"net"
	"strconv"

	"github.com/tiglabs/baudstorage/proto"
	"github.com/tiglabs/raft/util/log"
)

// Handle OpCreate Inode
func (m *MetaNode) opCreateInode(conn net.Conn, p *Packet) (err error) {
	req := &CreateInoReq{}
	if err = json.Unmarshal(p.Data, req); err != nil {
		return
	}
	mp, err := m.metaManager.LoadMetaPartition(req.PartitionID)
	if err != nil {
		return
	}
	if m.ProxyServe(conn, mp, p) {
		return
	}
	if err = mp.CreateInode(req, p); err != nil {
		log.Error("Create Inode Request: %s", err.Error())
	}
	// Reply operation result to client though TCP connection.
	err = m.replyClient(conn, p)
	return
}

// Handle OpCreateDentry
func (m *MetaNode) opCreateDentry(conn net.Conn, p *Packet) (err error) {
	req := &CreateDentryReq{}
	if err = json.Unmarshal(p.Data, req); err != nil {
		return
	}
	mp, err := m.metaManager.LoadMetaPartition(req.PartitionID)
	if err != nil {
		return err
	}
	if m.ProxyServe(conn, mp, p) {
		return
	}
	err = mp.CreateDentry(req, p)
	if err != nil {
		log.Error("Create Dentry: %s", err.Error())
	}
	// Reply operation result to client though TCP connection.
	err = m.replyClient(conn, p)
	return
}

// Handle OpDelete Dentry
func (m *MetaNode) opDeleteDentry(conn net.Conn, p *Packet) (err error) {
	req := &DeleteDentryReq{}
	if err = json.Unmarshal(p.Data, req); err != nil {
		return
	}
	mp, err := m.metaManager.LoadMetaPartition(req.PartitionID)
	if err != nil {
		return
	}
	if m.ProxyServe(conn, mp, p) {
		return
	}
	err = mp.DeleteDentry(req, p)
	if err != nil {
		return
	}
	// Reply operation result to client though TCP connection.
	err = m.replyClient(conn, p)
	return
}

func (m *MetaNode) opDeleteInode(conn net.Conn, p *Packet) (err error) {
	req := &DeleteInoReq{}
	if err = json.Unmarshal(p.Data, req); err != nil {
		return
	}
	mp, err := m.metaManager.LoadMetaPartition(req.PartitionID)
	if err != nil {
		return
	}
	if m.ProxyServe(conn, mp, p) {
		return
	}
	err = mp.DeleteInode(req, p)
	m.replyClient(conn, p)
	return
}

// Handle OpReadDir
func (m *MetaNode) opReadDir(conn net.Conn, p *Packet) (err error) {
	req := &proto.ReadDirRequest{}
	if err = json.Unmarshal(p.Data, req); err != nil {
		return
	}
	mp, err := m.metaManager.LoadMetaPartition(req.PartitionID)
	if err != nil {
		return
	}
	if m.ProxyServe(conn, mp, p) {
		return
	}
	err = mp.ReadDir(req, p)
	if err != nil {
		return
	}
	// Reply operation result to client though TCP connection.
	err = m.replyClient(conn, p)
	return
}

// Handle OpOpen
func (m *MetaNode) opOpen(conn net.Conn, p *Packet) (err error) {
	req := &proto.OpenRequest{}
	if err = json.Unmarshal(p.Data, req); err != nil {
		return
	}
	mp, err := m.metaManager.LoadMetaPartition(req.PartitionID)
	if err != nil {
		return
	}
	if m.ProxyServe(conn, mp, p) {
		return
	}
	err = mp.Open(req, p)
	if err != nil {
		return
	}
	// Reply operation result to client though TCP connection.
	err = m.replyClient(conn, p)
	return
}

func (m *MetaNode) opMetaInodeGet(conn net.Conn, p *Packet) (err error) {
	req := &proto.InodeGetRequest{}
	if err = json.Unmarshal(p.Data, req); err != nil {
		return
	}
	mp, err := m.metaManager.LoadMetaPartition(req.PartitionID)
	if err != nil {
		return
	}
	if m.ProxyServe(conn, mp, p) {
		return
	}
	err = mp.InodeGet(req, p)
	if err != nil {
		return
	}
	m.replyClient(conn, p)
	return
}

func (m *MetaNode) opMetaLookup(conn net.Conn, p *Packet) (err error) {
	req := &proto.LookupRequest{}
	if err = json.Unmarshal(p.Data, req); err != nil {
		return
	}
	mp, err := m.metaManager.LoadMetaPartition(req.PartitionID)
	if err != nil {
		return
	}
	if m.ProxyServe(conn, mp, p) {
		return
	}
	parentID, err := strconv.ParseUint(req.PartitionID, 10, 64)
	if err != nil {
		p.PackErrorWithBody(proto.OpErr, nil)
		return
	}
	dentry := &Dentry{
		ParentId: parentID,
		Name:     req.Name,
	}
	status := mp.getDentry(dentry)
	p.PackErrorWithBody(status, nil)
	return
}

func (m *MetaNode) opMetaExtentsAdd(conn net.Conn, p *Packet) (err error) {
	req := &proto.AppendExtentKeyRequest{}
	if err = json.Unmarshal(p.Data, req); err != nil {
		return
	}
	mp, err := m.metaManager.LoadMetaPartition(req.PartitionID)
	if err != nil {
		return
	}
	if m.ProxyServe(conn, mp, p) {
		return
	}
	err = mp.ExtentAppend(req, p)
	m.replyClient(conn, p)
	return
}

func (m *MetaNode) opMetaExtentsList(conn net.Conn, p *Packet) (err error) {
	req := &proto.GetExtentsRequest{}
	if err = json.Unmarshal(p.Data, req); err != nil {
		return
	}
	mp, err := m.metaManager.LoadMetaPartition(req.PartitionID)
	if err != nil {
		return
	}
	if m.ProxyServe(conn, mp, p) {
		return
	}

	if err = mp.ExtentsList(req, p); err != nil {
		//TODO: log
	}
	m.replyClient(conn, p)
	return
}

func (m *MetaNode) opMetaExtentsDel(conn net.Conn, p *Packet) (err error) {
	return
}

func (m *MetaNode) ProxyServe(conn net.Conn, mp *MetaPartition,
	p *Packet) bool {
	var (
		mConn      net.Conn
		leaderAddr string
		err        error
	)
	leaderAddr, ok := mp.isLeader()
	if ok {
		return false
	}
	if leaderAddr == "" {
		err = ErrNonLeader
		goto errEnd
	}
	// Get Master Conn
	mConn, err = m.pool.Get(leaderAddr)
	if err != nil {
		goto errEnd
	}
	defer func() {
		m.pool.Put(mConn)
	}()
	// Send Master Conn
	if err = p.WriteToConn(mConn); err != nil {
		goto errEnd
	}
	// Read conn from master
	if err = p.ReadFromConn(mConn, proto.NoReadDeadlineTime); err != nil {
		goto errEnd
	}
	if err = p.WriteToConn(conn); err != nil {
		log.Error("send to client: %s", err.Error())
		return false
	}
errEnd:
	p.PackErrorWithBody(proto.OpErr, nil)
	p.WriteToConn(conn)
	if err != nil {
		log.Error("proxy to master: %s", err.Error())
	}
	return false
}
