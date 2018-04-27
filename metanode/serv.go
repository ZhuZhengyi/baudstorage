package metanode

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"time"

	"github.com/juju/errors"
	"github.com/tiglabs/baudstorage/master"
	"github.com/tiglabs/baudstorage/proto"
	"github.com/tiglabs/baudstorage/util"
	"github.com/tiglabs/baudstorage/util/log"
)

// StartTcpService bind and listen specified port and accept tcp connections.
func (m *MetaNode) startTcpService() (err error) {
	// Init and start server.
	ln, err := net.Listen("tcp", m.addr)
	if err != nil {
		return
	}
	// Start goroutine for tcp accept handing.
	go func(ctx context.Context) {
		defer ln.Close()
		for {
			conn, err := ln.Accept()
			select {
			case <-ctx.Done():
				return
			default:
			}
			if err != nil {
				continue
			}
			// Start a goroutine for tcp connection handling.
			go m.serveTcpConn(conn, ctx)
		}
	}(m.ctx)
	return
}

// ServeTcpConn read data from specified tco connection until connection
// closed by remote or tcp service have been shutdown.
func (m *MetaNode) serveTcpConn(conn net.Conn, ctx context.Context) {
	defer conn.Close()
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}
		p := &Packet{}
		if err := p.ReadFromConn(conn, proto.ReadDeadlineTime*time.Second); err != nil {
			log.LogError("serve MetaNode: ", err.Error())
			return
		}
		// Start a goroutine for packet handling.
		go func() {
			if err := m.routePacket(conn, p); err != nil {
				log.LogError("serve operatorPkg: ", err.Error())
				return
			}
		}()
	}
}

// RoutePacket check the OpCode in specified packet and route it to handler.
func (m *MetaNode) routePacket(conn net.Conn, p *Packet) (err error) {
	switch p.Opcode {
	case proto.OpMetaCreate:
		// Client → MetaNode
		err = m.opCreate(conn, p)
	case proto.OpMetaRename:
		// Client → MetaNode
		err = m.opRename(conn, p)
	case proto.OpMetaDelete:
		// Client → MetaNode
		err = m.opDelete(conn, p)
	case proto.OpMetaReadDir:
		// Client → MetaNode
		err = m.opReadDir(conn, p)
	case proto.OpMetaOpen:
		// Client → MetaNode
		err = m.opOpen(conn, p)
	case proto.OpMetaCreateMetaRange:
		// Mater → MetaNode
		err = m.opCreateMetaRange(conn, p)
	default:
		// Unknown operation
		err = errors.New("unknown Opcode: " + proto.GetOpMesg(p.Opcode))
	}
	return
}

// ReplyToClient send reply data though tcp connection to client.
func (m *MetaNode) replyToClient(conn net.Conn, p *Packet, data interface{}) (err error) {
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
	jsonBytes, err := json.Marshal(data)
	if err != nil {
		return
	}
	p.Data = jsonBytes
	err = p.WriteToConn(conn)
	return
}

// ReplyToMaster reply operation result to master by sending http request.
func (m *MetaNode) replyToMaster(ip string, data interface{}) (err error) {
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
	// Process data and send reply though http specified remote address.
	jsonBytes, err := json.Marshal(data)
	if err != nil {
		return
	}
	url := fmt.Sprintf("http://%s%s", ip, master.MetaNodeResponse)
	util.PostToNode(jsonBytes, url)
	return
}

// Handle OpCreate
func (m *MetaNode) opCreateDentry(conn net.Conn, p *Packet) (err error) {
	req := &createDentryReq{}
	if err = json.Unmarshal(p.Data, req); err != nil {
		return
	}
	mr, err := m.metaRangeGroup.LoadMetaRange(req.Namespace)
	if err != nil {
		return err
	}
	resp := mr.CreateDentry(req)
	// Reply operation result to client though TCP connection.
	err = m.replyToClient(conn, p, resp)
	return
}

// Handle OpDelete Dentry
func (m *MetaNode) opDeleteDentry(conn net.Conn, p *Packet) (err error) {
	req := &deleteDentryReq{}
	if err = json.Unmarshal(p.Data, req); err != nil {
		return
	}
	mr, err := m.metaRangeGroup.LoadMetaRange(req.Namespace)
	if err != nil {
		return
	}
	resp := mr.DeleteDentry(req)
	// Reply operation result to client though TCP connection.
	err = m.replyToClient(conn, p, resp)
	return
}

// Handle OpCreate Inode
func (m *MetaNode) opCreateInode(conn net.Conn, p *Packet) (err error) {
	req := &createInoReq{}
	if err = json.Unmarshal(p.Data, req); err != nil {
		return
	}
	mr, err := m.metaRangeGroup.LoadMetaRange(req.Namespace)
	if err != nil {
		return
	}
	resp := mr.CreateInode(req)
	// Reply operation result to client though TCP connection.
	err = m.replyToClient(conn, p, resp)
	return
}

func (m *MetaNode) opDeleteInode(conn net.Conn, p *Packet) (err error) {
	req := &deleteInoReq{}
	if err = json.Unmarshal(p.Data, req); err != nil {
		return
	}
	mr, err := m.metaRangeGroup.LoadMetaRange(req.Namespace)
	if err != nil {
		return
	}
	resp := mr.DeleteInode(req)
	err = m.replyToClient(conn, p, resp)
	return
}

// Handle OpReadDir
func (m *MetaNode) opReadDir(conn net.Conn, p *Packet) (err error) {
	req := &proto.ReadDirRequest{}
	if err = json.Unmarshal(p.Data, req); err != nil {
		return
	}
	mr, err := m.metaRangeGroup.LoadMetaRange(req.Namespace)
	if err != nil {
		return
	}
	resp := mr.ReadDir(req)
	// Reply operation result to client though TCP connection.
	err = m.replyToClient(conn, p, resp)
	return
}

// Handle OpOpen
func (m *MetaNode) opOpen(conn net.Conn, p *Packet) (err error) {
	req := &proto.OpenRequest{}
	if err = json.Unmarshal(p.Data, req); err != nil {
		return
	}
	mr, err := m.metaRangeGroup.LoadMetaRange(req.Namespace)
	if err != nil {
		return
	}
	resp := mr.Open(req)
	// Reply operation result to client though TCP connection.
	err = m.replyToClient(conn, p, resp)
	return
}

// Handle OpCreateMetaRange
func (m *MetaNode) opCreateMetaRange(conn net.Conn, p *Packet) (err error) {
	remoteAddr := conn.RemoteAddr()
	m.masterAddr = net.ParseIP(remoteAddr.String()).String()
	defer func() {
		resp := &proto.CreateMetaRangeResponse{}
		if err != nil {
			// Operation failure.
			resp.Status = proto.OpErr
			resp.Result = err.Error()
			m.replyToMaster(m.masterAddr, resp)
		} else {
			// Operation success.
			resp.Status = proto.OpOk
			m.replyToMaster(m.masterAddr, resp)
		}
	}()
	// Get task from packet.
	adminTask := &proto.AdminTask{}
	if err = json.Unmarshal(p.Data, adminTask); err != nil {
		return
	}
	// Get request detail from task.
	req, ok := adminTask.Request.(proto.CreateMetaRangeRequest)
	if !ok {
		errors.New("invalid request body")
		return
	}
	mr := NewMetaRange(req.MetaId, req.Start, req.End, req.Members)
	// Store MetaRange to group.
	m.metaRangeGroup.StoreMetaRange(req.MetaId, mr)
	return
}
