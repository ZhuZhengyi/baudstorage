package metanode

import (
	"encoding/json"
	"net"

	"github.com/tiglabs/baudstorage/proto"
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
	req := &proto.CreateMetaRangeRequest{}
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
