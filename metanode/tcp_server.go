package metanode

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"time"

	"errors"
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
		// Start a goroutine for packet handling. Do not block connection read goroutine.
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
	case proto.OpMetaCreateInode:
		// Client → MetaNode
		err = m.opCreateInode(conn, p)
	case proto.OpMetaCreateDentry:
		// Client → MetaNode
		err = m.opCreateDentry(conn, p)
	case proto.OpMetaDeleteInode:
		// Client → MetaNode
		err = m.opDeleteInode(conn, p)
	case proto.OpMetaDeleteDentry:
		// Client → MetaNode
		err = m.opDeleteDentry(conn, p)
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