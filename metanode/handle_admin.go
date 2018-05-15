package metanode

import (
	"encoding/json"
	"fmt"
	"net"
	"os"
	"path"

	"github.com/henrylee2cn/goutil/errors"
	"github.com/tiglabs/baudstorage/proto"
)

// Handle OpCreateMetaRange
func (m *MetaNode) opCreateMetaRange(conn net.Conn, p *Packet) (err error) {
	req := &proto.CreateMetaRangeRequest{}
	// Get task from packet.
	adminTask := &proto.AdminTask{}
	if err = json.Unmarshal(p.Data, adminTask); err != nil {
		return
	}
	defer func() {
		// Response task result to master.
		resp := &proto.CreateMetaRangeResponse{}
		resp.GroupId = req.GroupId
		if err != nil {
			// Operation failure.
			resp.Status = proto.OpErr
			resp.Result = err.Error()
		} else {
			// Operation success.
			resp.Status = proto.OpOk
		}
		adminTask.Response = resp
		adminTask.Request = nil
		m.replyToMaster(m.masterAddr, adminTask)
	}()
	// Marshal request body.
	requestJson, err := json.Marshal(adminTask.Request)
	if err != nil {
		return
	}
	// Unmarshal request to entity
	if err := json.Unmarshal(requestJson, req); err != nil {
		return
	}
	// Create new  MetaRange.
	id := fmt.Sprintf("%d", req.GroupId)
	mConf := MetaRangeConfig{
		ID:          id,
		Start:       req.Start,
		End:         req.End,
		Cursor:      req.Start,
		RaftGroupID: req.GroupId,
		Peers:       req.Members,
		RootDir:     path.Join(m.metaDir, id),
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
	// Create metaRange base dir
	if _, err = os.Stat(mr.RootDir); err == nil {
		err = errors.New(fmt.Sprint("metaRange root dir '%s' is exsited!",
			mr.RootDir))
		return
	}
	os.MkdirAll(mr.RootDir, 0755)
	defer func() {
		if err != nil {
			os.RemoveAll(mr.RootDir)
		}
	}()
	// Write metaRange to file
	if err = mr.StoreMeta(); err != nil {
		return
	}
	//TODO: create Raft
	if err = m.createPartition(mr); err != nil {
		return
	}
	return
}

func (m *MetaNode) opHeartBeatRequest(conn net.Conn, p *Packet) (err error) {
	// Ack from Master Request

	return
}
