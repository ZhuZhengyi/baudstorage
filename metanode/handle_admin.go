package metanode

import (
	"encoding/json"
	"fmt"
	"github.com/juju/errors"
	"github.com/tiglabs/baudstorage/master"
	"github.com/tiglabs/baudstorage/util"
	"net"

	"github.com/tiglabs/baudstorage/proto"
)

// Handle OpCreateMetaRange
func (m *MetaNode) opCreateMetaRange(conn net.Conn, p *Packet) (err error) {
	// Ack to master
	go m.ackAdmin(conn, p)
	remoteAddr := conn.RemoteAddr()
	m.masterAddr = net.ParseIP(remoteAddr.String()).String()
	// Get task from packet.
	adminTask := &proto.AdminTask{}
	if err = json.Unmarshal(p.Data, adminTask); err != nil {
		return
	}
	defer func() {
		// Response task result to master.
		resp := &proto.CreateMetaPartitionResponse{}
		if err != nil {
			// Operation failure.
			resp.Status = proto.OpErr
			resp.Result = err.Error()
		} else {
			// Operation success.
			resp.Status = proto.OpOk
		}
		adminTask.Response = resp
		m.replyAdmin(m.masterAddr, adminTask)
	}()
	// Marshal request body.
	requestJson, err := json.Marshal(adminTask.Request)
	if err != nil {
		return
	}
	// Unmarshal request to entity
	req := &proto.CreateMetaPartitionRequest{}
	if err := json.Unmarshal(requestJson, req); err != nil {
		return
	}
	// Create new  MetaRange.
	mConf := MetaRangeConfig{
		ID:          req.MetaId,
		Start:       req.Start,
		End:         req.End,
		Cursor:      req.Start,
		RaftGroupID: req.GroupId,
		Peers:       req.Members,
	}
	mr := NewMetaRange(mConf)
	if err = m.metaRangeManager.SetMetaRange(mr); err != nil {
		return
	}
	defer func() {
		if err != nil {
			m.metaRangeManager.DeleteMetaRange(mr.ID)
		}
	}()
	//TODO: Write to File

	//TODO: create Raft

	return
}

// ReplyToMaster reply operation result to master by sending http request.
func (m *MetaNode) replyAdmin(ip string, data interface{}) (err error) {
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

func (m *MetaNode) ackAdmin(conn net.Conn, p *Packet) (err error) {
	p.PackOkReply()
	err = p.WriteToConn(conn)
	return
}
