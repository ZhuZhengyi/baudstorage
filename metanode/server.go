package metanode

import (
	"io"
	"net"
	"strconv"

	"github.com/tiglabs/baudstorage/proto"
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
	log.LogDebugf("start Server over...")
	return
}

func (m *MetaNode) stopServer() {
	if m.httpStopC != nil {
		defer func() {
			if r := recover(); r != nil {
				log.LogErrorf("action[StopTcpServer],err:%v", r)
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
		if err := p.ReadFromConn(conn, proto.NoReadDeadlineTime); err != nil {
			if err != io.EOF {
				log.LogError("serve MetaNode: ", err.Error())
			}
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
	// Handle request
	err = m.metaManager.HandleMetaOperation(conn, p)
	return
}
