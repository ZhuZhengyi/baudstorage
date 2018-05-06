package metanode

import (
	"encoding/json"
	"github.com/tiglabs/baudstorage/proto"
	"net"
)

// Handle OpCreateMetaRange
func (m *MetaNode) opCreateMetaRange(conn net.Conn, p *Packet) (err error) {
	remoteAddr := conn.RemoteAddr()
	m.masterAddr = net.ParseIP(remoteAddr.String()).String()
	// Get task from packet.
	adminTask := &proto.AdminTask{}
	if err = json.Unmarshal(p.Data, adminTask); err != nil {
		return
	}
	defer func() {
		// Response task result to master.
		resp := &proto.CreateMetaRangeResponse{}
		if err != nil {
			// Operation failure.
			resp.Status = proto.OpErr
			resp.Result = err.Error()
		} else {
			// Operation success.
			resp.Status = proto.OpOk
		}
		adminTask.Response = resp
		m.replyToMaster(m.masterAddr, adminTask)
	}()
	// Marshal request body.
	requestJson, err := json.Marshal(adminTask.Request)
	if err != nil {
		return
	}
	// Unmarshal request to entity
	request := &proto.CreateMetaRangeRequest{}
	if err := json.Unmarshal(requestJson, request); err != nil {
		return
	}
	// Create new  MetaRange.
	m.metaRangeManager.CreateMetaRange(request.MetaId, request.Start, request.End, request.Members)
	return
}
