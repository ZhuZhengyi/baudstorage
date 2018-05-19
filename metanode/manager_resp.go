package metanode

import (
	"encoding/json"
	"fmt"
	"github.com/juju/errors"
	"github.com/tiglabs/baudstorage/util"
	"net"
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
	url := fmt.Sprintf("http://%s%s", ip, masterResponsePath)
	util.PostToNode(jsonBytes, url)
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
	return
}
