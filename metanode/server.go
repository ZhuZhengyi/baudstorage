package metanode

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
	"time"

	"github.com/tiglabs/baudstorage/proto"
	"github.com/tiglabs/baudstorage/util"
	"github.com/tiglabs/baudstorage/util/log"
)

// StartTcpService bind and listen specified port and accept tcp connections.
func (m *MetaNode) startServer() (err error) {
	// Init and start server.
	m.httpStopC = make(chan uint8)
	ln, err := net.Listen("tcp", ":"+strconv.Itoa(m.listen))
	if err != nil {
		return
	}
	// Start goroutine for tcp accept handing.
	go func(stopC chan uint8) {
		defer ln.Close()
		for {
			conn, err := ln.Accept()
			select {
			case <-stopC:
				return
			default:
			}
			if err != nil {
				continue
			}
			// Start a goroutine for tcp connection handling.
			go m.servConn(conn, stopC)
		}
	}(m.httpStopC)
	return
}

func (m *MetaNode) stopTcpServer() {
	if m.httpStopC != nil {
		defer func() {
			if r := recover(); r != nil {
				log.LogError("action[StopTcpServer],err:%v", r)
			}
		}()
		close(m.httpStopC)
	}
}

// ServeTcpConn read data from specified tco connection until connection
// closed by remote or tcp service have been shutdown.
func (m *MetaNode) servConn(conn net.Conn, stopC chan uint8) {
	defer conn.Close()
	for {
		select {
		case <-stopC:
			return
		default:
		}
		p := &Packet{}
		if err := p.ReadFromConn(conn, proto.ReadDeadlineTime*time.Second); err != nil && err != io.EOF {
			log.LogError("serve MetaNode: ", err.Error())
			return
		}
		// Start a goroutine for packet handling. Do not block connection read goroutine.
		go func() {
			if err := m.handlePacket(conn, p); err != nil {
				log.LogError("serve operatorPkg: ", err.Error())
				return
			}
		}()
	}
}

// RoutePacket check the OpCode in specified packet and route it to handler.
func (m *MetaNode) handlePacket(conn net.Conn, p *Packet) (err error) {
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
	case proto.OpHeartBeatRequest:
		err = m.opHeartBeatRequest(conn, p)
	default:
		// Unknown operation
		err = errors.New("unknown Opcode: " + proto.GetOpMesg(p.Opcode))
	}
	return
}

// ReplyToClient send reply data though tcp connection to client.
func (m *MetaNode) replyToClient(conn net.Conn, p *Packet, data []byte) (err error) {
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
	p.Data = data
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
	url := fmt.Sprintf("http://%s/%s", ip, metaNodeResponse)
	util.PostToNode(jsonBytes, url)
	return
}
