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

func (m *MetaNode) startTcpService(ctx context.Context) (err error) {
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
	}(ctx)
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
	case proto.OpCreate:
		err = m.opCreate(conn, p)
	case proto.OpRename:
	case proto.OpDelete:
	case proto.OpList:
	case proto.OpOpen:
	case proto.OpCreateMetaRange:
		err = m.createMetaRange(conn, p)
	default:
		err = errors.New("unknown Opcode: " + proto.GetOpMesg(p.Opcode))
	}
	return
}

// handle OpCreate
func (m *MetaNode) opCreate(conn net.Conn, p *Packet) (err error) {
	request := new(proto.CreateRequest)
	if err = json.Unmarshal(p.Data, request); err != nil {
		return
	}
	var mr *MetaRange
	if v, ok := m.metaRanges.Load(request.Namespace); !ok {
		err = errors.New("unknown namespace: " + request.Namespace)
		return
	} else {
		mr = v.(*MetaRange)
	}

	response := mr.Create(request)
	//FIXME: HTTP Response

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
	response := proto.CreateMetaRangeResponse{
		Status: proto.OpOk,
		Result: proto.GetOpMesg(proto.OpOk),
	}
	//task response

	return
}
