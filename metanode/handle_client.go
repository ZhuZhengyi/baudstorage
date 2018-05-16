package metanode

import (
	"encoding/json"
	"errors"
	"net"
	"time"

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
	if ok := m.ProxyServe(conn, mp, p); !ok {
		return
	}
	if err = mp.CreateInode(req, p); err != nil {
		log.Error("Create Inode Request: %s", err.Error())
	}
	// Reply operation result to client though TCP connection.
	err = m.replyClient(conn, p)
	return
}

// Handle OpCreate
func (m *MetaNode) opCreateDentry(conn net.Conn, p *Packet) (err error) {
	req := &CreateDentryReq{}
	if err = json.Unmarshal(p.Data, req); err != nil {
		return
	}
	mp, err := m.metaManager.LoadMetaPartition(req.PartitionID)
	if err != nil {
		return err
	}
	if ok := m.ProxyServe(conn, mp, p); !ok {
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
	if ok := m.ProxyServe(conn, mp, p); !ok {
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
	if ok := m.ProxyServe(conn, mp, p); !ok {
		return
	}
	err = mp.DeleteInode(req, p)
	if err != nil {
		return
	}
	err = m.replyClient(conn, p)
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
	if ok := m.ProxyServe(conn, mp, p); !ok {
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
	if ok := m.ProxyServe(conn, mp, p); !ok {
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

// ReplyToClient send reply data though tcp connection to client.
func (m *MetaNode) replyClient(conn net.Conn, p *Packet) (err error) {
	// Handle panic
	defer func() {
		if r := recover(); r != nil {
			switch data := r.(type) {
			case error:
				err = data
			default:
				err = errors.New(data.(string))
			}
		}
	}()
	// Process data and send reply though specified tcp connection.
	err = p.WriteToConn(conn)
	return
}

func (m *MetaNode) ProxyServe(conn net.Conn, mp *MetaPartition,
	p *Packet) (ok bool) {
	var (
		mConn      net.Conn
		status     uint8
		leaderAddr string
		err        error
	)
	if leaderAddr, ok = mp.isLeader(); ok {
		return
	}
	if leaderAddr == "" {
		err = ErrNonLeader
		status = proto.OpErr
		goto end
	}
	// Get Master Conn
	mConn, err = m.proxyPool.Get(leaderAddr)
	if err != nil {
		status = proto.OpErr
		goto end
	}
	defer func() {
		m.proxyPool.Put(mConn)
	}()
	// Send Master Conn
	if err = p.WriteToConn(mConn); err != nil {
		status = proto.OpErr
		goto end
	}
	// Read conn from master
	if err = p.ReadFromConn(mConn, proto.ReadDeadlineTime*time.
		Second); err != nil {
		status = proto.OpErr
		goto end
	}
end:
	p.PackErrorWithBody(status, nil)
	p.WriteToConn(conn)
	if err != nil {
		log.Error("proxy to master: %s", err.Error())
	}
	return
}
