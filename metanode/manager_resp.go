package metanode

import (
	"encoding/json"
	"fmt"
	"net"

	"github.com/juju/errors"
	"github.com/tiglabs/baudstorage/util"
	"github.com/tiglabs/baudstorage/util/log"
)

const (
	masterResponsePath = "metaNode/response" // Method: 'POST', ContentType: 'application/json'
)

// ReplyToMaster reply operation result to master by sending http request.
func (m *metaManager) respondToMaster(ip string, data interface{}) (err error) {
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
	url := fmt.Sprintf("http://%s/%s", ip, masterResponsePath)
	_, err = util.PostToNode(jsonBytes, url)
	if err != nil {
		log.LogErrorf("response to master: %s", err.Error())
	}
	return
}

// ReplyToClient send reply data though tcp connection to client.
func (m *metaManager) respondToClient(conn net.Conn, p *Packet) (err error) {
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
	if err != nil {
		log.LogErrorf("response to client: %s", err.Error())
	}
	return
}

func (m *metaManager) responseAckOKToMaster(conn net.Conn, p *Packet) {
	go func() {
		p.PackOkReply()
		if err := p.WriteToConn(conn); err != nil {
			log.LogErrorf("ack master response: %s", err.Error())
		}
	}()
}
