package metanode

import (
	"encoding/json"
	"errors"
	"net"

	"github.com/tiglabs/baudstorage/proto"
	"github.com/tiglabs/raft/util/log"
)

// Handle OpCreate Inode
func (m *MetaNode) opCreateInode(conn net.Conn, p *Packet) (err error) {
	req := &CreateInoReq{}
	if err = json.Unmarshal(p.Data, req); err != nil {
		return
	}
	mr, err := m.metaRangeManager.LoadMetaRange(req.PartitionID)
	if err != nil {
		return
	}
	if err = mr.CreateInode(req, p); err != nil {
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
	mr, err := m.metaRangeManager.LoadMetaRange(req.PartitionID)
	if err != nil {
		return err
	}
	err = mr.CreateDentry(req, p)
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
	mr, err := m.metaRangeManager.LoadMetaRange(req.PartitionID)
	if err != nil {
		return
	}
	err = mr.DeleteDentry(req, p)
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
	mr, err := m.metaRangeManager.LoadMetaRange(req.PartitionID)
	if err != nil {
		return
	}
	err = mr.DeleteInode(req, p)
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
	mr, err := m.metaRangeManager.LoadMetaRange(req.PartitionID)
	if err != nil {
		return
	}
	err = mr.ReadDir(req, p)
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
	mr, err := m.metaRangeManager.LoadMetaRange(req.PartitionID)
	if err != nil {
		return
	}
	err = mr.Open(req, p)
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
