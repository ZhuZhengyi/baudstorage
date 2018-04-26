package metanode

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/tiglabs/baudstorage/proto"
	"github.com/tiglabs/baudstorage/util/log"
	"net"
	"time"
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
		err = m.create(conn, p)
	case proto.OpMetaRename:
		err = m.rename(conn, p)
	case proto.OpMetaDelete:
		err = m.delete(conn, p)
	case proto.OpMetaReadDir:
		err = m.clientReadDir(conn, p)
	case proto.OpMetaOpen:
	case proto.OpMetaCreateMetaRange:
		err = m.createMetaRange(conn, p)
	default:
		err = errors.New("unknown Opcode: " + proto.GetOpMesg(p.Opcode))
	}
	return
}

// handle OpCreate
func (m *MetaNode) create(conn net.Conn, p *Packet) (err error) {
	req := new(proto.CreateRequest)
	if err = json.Unmarshal(p.Data, req); err != nil {
		return
	}
	var mr *MetaRange
	if v, ok := m.metaRanges.Load(req.Namespace); !ok {
		err = errors.New("unknown namespace: " + req.Namespace)
		return
	} else {
		mr = v.(*MetaRange)
	}

	response := mr.Create(req)
	//FIXME: HTTP Response

	return
}

func (m *MetaNode) rename(conn net.Conn, p *Packet) (err error) {
	req := new(proto.RenameRequest)
	if err = json.Unmarshal(p.Data, req); err != nil {
		return
	}
	return
}

func (m *MetaNode) delete(conn net.Conn, p *Packet) (err error) {
	req := new(proto.DeleteRequest)
	if err = json.Unmarshal(p.Data, req); err != nil {
		return
	}
	return
}

func (m *MetaNode) clientReadDir(conn net.Conn, p *Packet) (err error) {
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
