package metanode

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"strings"
	"time"

	"github.com/juju/errors"
	"github.com/tiglabs/baudstorage/proto"
	"github.com/tiglabs/baudstorage/util/log"
)

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
			go m.serveTCPConn(conn, ctx)
		}
	}(m.ctx)
	return
}

func (m *MetaNode) serveTCPConn(conn net.Conn, ctx context.Context) {
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
			if err := m.operatorPkg(conn, p); err != nil {
				log.LogError("serve operatorPkg: ", err.Error())
				return
			}
		}()
	}
}

func (m *MetaNode) operatorPkg(conn net.Conn, p *Packet) (err error) {
	switch p.Opcode {
	case proto.OpMetaCreate:
		// Client → MetaNode
		err = m.create(conn, p)
	case proto.OpMetaRename:
		// Client → MetaNode
		err = m.rename(conn, p)
	case proto.OpMetaDelete:
		// Client → MetaNode
		err = m.delete(conn, p)
	case proto.OpMetaReadDir:
		// Client → MetaNode
		err = m.readDir(conn, p)
	case proto.OpMetaOpen:
		// Client → MetaNode
		err = m.open(conn, p)
	case proto.OpMetaCreateMetaRange:
		// Mater → MetaNode
		err = m.createMetaRange(conn, p)
	default:
		// Unknown
		err = errors.New("unknown Opcode: " + proto.GetOpMesg(p.Opcode))
	}
	return
}

// Handle OpCreate
func (m *MetaNode) create(conn net.Conn, p *Packet) (err error) {
	req := new(proto.CreateRequest)
	if err = json.Unmarshal(p.Data, req); err != nil {
		return
	}
	var mr *MetaRange
	if v, ok := m.metaRanges.Load(req.Namespace); !ok {
		err = m.newUnknownNamespaceErr(req.Namespace)
		return
	} else {
		mr = v.(*MetaRange)
	}

	resp := mr.Create(req)
	respJson, err := json.Marshal(resp)
	if err != nil {
		return
	}
	// Reply operation result to client though TCP connection.
	p.Data = respJson
	err = p.WriteToConn(conn)
	return
}

// Handle OpRename
func (m *MetaNode) rename(conn net.Conn, p *Packet) (err error) {
	req := new(proto.RenameRequest)
	if err = json.Unmarshal(p.Data, req); err != nil {
		return
	}
	val, has := m.metaRanges.Load(req.Namespace)
	if !has {
		err = m.newUnknownNamespaceErr(req.Namespace)
		return
	}
	mr := val.(*MetaRange)
	resp := mr.Rename(req)
	respJson, err := json.Marshal(resp)
	if err != nil {
		return
	}
	// Reply operation result to client though TCP connection.
	p.Data = respJson
	err = p.WriteToConn(conn)
	return
}

func (m *MetaNode) delete(conn net.Conn, p *Packet) (err error) {
	req := new(proto.DeleteRequest)
	if err = json.Unmarshal(p.Data, req); err != nil {
		return
	}
	val, has := m.metaRanges.Load(req.Namespace)
	if !has {
		err = m.newUnknownNamespaceErr(req.Namespace)
		return
	}
	mr := val.(*MetaRange)
	resp := mr.Delete(req)
	p.Data, _ = json.Marshal(resp)
	err = p.WriteToConn(conn)
	return
}

func (m *MetaNode) readDir(conn net.Conn, p *Packet) (err error) {
	return
}

func (m *MetaNode) open(conn net.Conn, p *Packet) (err error) {
	return
}

func (m *MetaNode) createMetaRange(conn net.Conn,
	p *Packet) (err error) {
	{
		addr := conn.RemoteAddr().String()
		m.masterAddr = net.ParseIP(addr).String()
	}
	request := new(proto.CreateMetaRangeRequest)
	if err = json.Unmarshal(p.Data, request); err != nil {
		return
	}
	mr := NewMetaRange(request.MetaId, request.Start, request.End,
		request.Members)
	if _, ok := m.metaRanges.LoadOrStore(request.MetaId, mr); !ok {
		err = errors.New("metaNode createRange failed: " + request.MetaId)
	}
	//FIXME: HTTP Response

	return
}

func (m *MetaNode) loadMetaRange(namespace string) (mr *MetaRange, err error) {
	if len(strings.TrimSpace(namespace)) == 0 {
		err = m.newUnknownNamespaceErr("")
		return
	}
	// TODO: Not complete yet.
	return
}

func (m *MetaNode) newUnknownNamespaceErr(namespace string) (err error) {
	err = errors.New(fmt.Sprintf("unknown namespace %s", namespace))
	return
}
