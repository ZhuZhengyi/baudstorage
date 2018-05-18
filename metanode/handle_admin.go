package metanode

import (
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"path"

	"github.com/tiglabs/baudstorage/proto"
	"github.com/tiglabs/baudstorage/util"
	raftproto "github.com/tiglabs/raft/proto"
	"github.com/tiglabs/raft/util/log"
)

const (
	// Operation response
	metaNodeResponse = "metaNode/response" // Method: 'POST', ContentType: 'application/json'
)

// Handle OpCreateMetaPartition
func (m *MetaNode) opCreateMetaPartition(conn net.Conn, p *Packet) (err error) {
	// Get task from packet.
	adminTask := &proto.AdminTask{}
	if err = json.Unmarshal(p.Data, adminTask); err != nil {
		p.PackErrorWithBody(proto.OpErr, nil)
		m.ackAdmin(conn, p)
		return
	}

	// Marshal request body.
	requestJson, err := json.Marshal(adminTask.Request)
	if err != nil {
		p.PackErrorWithBody(proto.OpErr, nil)
		m.ackAdmin(conn, p)
		return
	}
	// Unmarshal request to entity
	req := &proto.CreateMetaPartitionRequest{}
	if err := json.Unmarshal(requestJson, req); err != nil {
		p.PackErrorWithBody(proto.OpErr, nil)
		m.ackAdmin(conn, p)
		return
	}
	// Ack Master Request
	go func() {
		p.PackOkReply()
		m.ackAdmin(conn, p)
	}()
	defer func() {
		// Response task result to master.
		resp := &proto.CreateMetaPartitionResponse{
			NsName:      req.NsName,
			PartitionID: req.PartitionID,
		}
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
	// Create new  MetaPartition.
	id := fmt.Sprintf("%d", req.PartitionID)
	mConf := MetaPartitionConfig{
		ID:          id,
		Start:       req.Start,
		End:         req.End,
		Cursor:      req.Start,
		RaftGroupID: req.PartitionID,
		Peers:       req.Members,
		RootDir:     path.Join(m.metaDir, id),
		MetaManager: m.metaManager,
	}
	mp := NewMetaPartition(mConf)
	if err = m.metaManager.SetMetaPartition(mp); err != nil {
		return
	}
	defer func() {
		if err != nil {
			m.metaManager.DeleteMetaPartition(mp.ID)
		}
	}()
	// Create metaPartition base dir
	if _, err = os.Stat(mp.RootDir); err == nil {
		err = errors.New(fmt.Sprint("metaPartition root dir '%s' is exsited", mp.RootDir))
		return
	}
	os.MkdirAll(mp.RootDir, 0755)
	defer func() {
		if err != nil {
			os.RemoveAll(mp.RootDir)
		}
	}()
	// Write metaPartition to file
	if err = mp.StoreMeta(); err != nil {
		return
	}
	// Create Raft instance
	if err = m.createPartition(mp); err != nil {
		return
	}
	go mp.StartStoreSchedule()
	return
}

func (m *MetaNode) opMetaNodeHeartbeat(conn net.Conn, p *Packet) (err error) {
	// Ack from Master Request
	adminTask := &proto.AdminTask{}
	if err = json.Unmarshal(p.Data, adminTask); err != nil {
		// TODO: Log
		adminTask.Status = proto.TaskFail
		goto end
	}
	// parse req
	{
		var (
			req     = &proto.HeartBeatRequest{}
			reqData []byte
		)
		reqData, err = json.Marshal(adminTask.Request)
		if err != nil {
			adminTask.Status = proto.TaskFail
			goto end
		}
		if err = json.Unmarshal(reqData, req); err != nil {
			adminTask.Status = proto.TaskFail
			goto end
		}
		m.masterAddr = req.MasterAddr
	}
	// collect used info
	if true {
		resp := &proto.MetaNodeHeartbeatResponse{}
		// machine mem total and used
		resp.Total, resp.Used, err = util.GetMemInfo()
		if err != nil {
			adminTask.Status = proto.TaskFail
			goto end
		}
		// every partition used
		m.metaManager.Range(func(id string, mp *MetaPartition) bool {
			mpr := &proto.MetaPartitionReport{}
			mpr.PartitionID = mp.RaftGroupID
			mpr.IsLeader = mp.RaftPartition.IsLeader()
			mpr.Status = 1
			mpr.Used = mp.Sizeof()
			resp.MetaPartitionInfo = append(resp.MetaPartitionInfo, mpr)
			return true
		})
		resp.Status = proto.OpOk
	}

end:
	err = m.replyToMaster(m.masterAddr, adminTask)
	return
}

func (m *MetaNode) opDeleteMetaPartition(conn net.Conn, p *Packet) (err error) {
	adminTask := &proto.AdminTask{}
	if err = json.Unmarshal(p.Data, adminTask); err != nil {
		p.PackErrorWithBody(proto.OpErr, nil)
		m.ackAdmin(conn, p)
		return
	}
	var (
		mp      *MetaPartition
		reqData []byte
	)
	req := &proto.DeleteMetaPartitionRequest{}
	reqData, err = json.Marshal(adminTask.Request)
	if err != nil {
		p.PackErrorWithBody(proto.OpErr, nil)
		m.ackAdmin(conn, p)
		return
	}
	if err = json.Unmarshal(reqData, req); err != nil {
		p.PackErrorWithBody(proto.OpErr, nil)
		m.ackAdmin(conn, p)
		return
	}
	resp := &proto.DeleteMetaPartitionResponse{
		PartitionID: req.PartitionID,
		Status:      proto.OpErr,
	}
	mp, err = m.metaManager.LoadMetaPartition(fmt.Sprintf("%d", req.PartitionID))
	if err != nil {
		p.PackErrorWithBody(proto.OpErr, nil)
		m.ackAdmin(conn, p)
		return
	}
	if m.ProxyServe(conn, mp, p) {
		return
	}
	// Ack Master Request
	go func() {
		p.PackOkReply()
		p.WriteToConn(conn)
	}()
	if err = mp.DeletePartition(); err != nil {
		resp.Result = err.Error()
		goto end
	}
	resp.Status = proto.OpOk
end:
	adminTask.Response = resp
	adminTask.Request = nil
	err = m.replyToMaster(m.masterAddr, adminTask)
	return
}

func (m *MetaNode) opUpdateMetaPartition(conn net.Conn, p *Packet) (err error) {
	adminTask := &proto.AdminTask{}
	if err = json.Unmarshal(p.Data, adminTask); err != nil {
		p.PackErrorWithBody(proto.OpErr, nil)
		m.ackAdmin(conn, p)
		return
	}
	var (
		reqData []byte
		req     = &proto.UpdateMetaPartitionRequest{}
	)
	reqData, err = json.Marshal(adminTask.Request)
	if err != nil {
		p.PackErrorWithBody(proto.OpErr, nil)
		m.ackAdmin(conn, p)
		return
	}
	if err = json.Unmarshal(reqData, req); err != nil {
		p.PackErrorWithBody(proto.OpErr, nil)
		m.ackAdmin(conn, p)
		return
	}
	var mp *MetaPartition
	mp, err = m.metaManager.LoadMetaPartition(fmt.Sprintf("%d", req.PartitionID))
	if err != nil {
		p.PackErrorWithBody(proto.OpErr, nil)
		m.ackAdmin(conn, p)
		return
	}
	if m.ProxyServe(conn, mp, p) {
		return
	}
	go func() {
		p.PackOkReply()
		p.WriteToConn(conn)
	}()
	resp := &proto.UpdateMetaPartitionResponse{
		NsName:      req.NsName,
		PartitionID: req.PartitionID,
		End:         req.End,
		Status:      proto.OpOk,
	}
	if err = mp.UpdatePartition(req); err != nil {
		resp.Status = proto.OpErr
	}
	adminTask.Response = resp
	adminTask.Request = nil
	err = m.replyToMaster(m.masterAddr, adminTask)
	return
}

func (m *MetaNode) opLoadMetaPartition(conn net.Conn, p *Packet) (err error) {
	adminTask := &proto.AdminTask{}
	if err = json.Unmarshal(p.Data, adminTask); err != nil {
		p.PackErrorWithBody(proto.OpErr, nil)
		p.WriteToConn(conn)
		return
	}
	var (
		req     = &proto.LoadMetaPartitionMetricRequest{}
		resp    = &proto.LoadMetaPartitionMetricResponse{}
		mp      *MetaPartition
		reqData []byte
	)
	if reqData, err = json.Marshal(adminTask.Request); err != nil {
		p.PackErrorWithBody(proto.OpErr, nil)
		p.WriteToConn(conn)
		return
	}
	if err = json.Unmarshal(reqData, req); err != nil {
		p.PackErrorWithBody(proto.OpErr, nil)
		p.WriteToConn(conn)
		return
	}
	go func() {
		p.PackOkReply()
		p.WriteToConn(conn)
	}()
	mp, err = m.metaManager.LoadMetaPartition(fmt.Sprintf("%d",
		req.PartitionID))
	if err != nil {
		resp.Status = proto.OpErr
		resp.Result = err.Error()
		adminTask.Response = resp
		goto end
	}
	resp.Start = mp.Start
	resp.End = mp.End
	resp.MaxInode = mp.Cursor
	resp.Status = proto.OpOk
	adminTask.Response = resp
end:
	adminTask.Request = nil
	err = m.replyToMaster(m.masterAddr, adminTask)
	return
}

func (m *MetaNode) opOfflineMetaPartition(conn net.Conn, p *Packet) (err error) {
	var (
		mp      *MetaPartition
		reqData []byte
		req     = &proto.MetaPartitionOfflineRequest{}
	)
	adminTask := &proto.AdminTask{}
	if err = json.Unmarshal(p.Data, adminTask); err != nil {
		p.PackErrorWithBody(proto.OpErr, nil)
		m.ackAdmin(conn, p)
		return
	}
	reqData, err = json.Marshal(adminTask.Request)
	if err != nil {
		p.PackErrorWithBody(proto.OpErr, nil)
		m.ackAdmin(conn, p)
		return
	}
	if err = json.Unmarshal(reqData, req); err != nil {
		p.PackErrorWithBody(proto.OpErr, nil)
		m.ackAdmin(conn, p)
		return
	}
	mp, err = m.metaManager.LoadMetaPartition(fmt.Sprintf("%d", req.PartitionID))
	if err != nil {
		p.PackErrorWithBody(proto.OpErr, nil)
		m.ackAdmin(conn, p)
		return
	}
	if m.ProxyServe(conn, mp, p) {
		return
	}
	defer func() {
		p.PackOkReply()
		m.ackAdmin(conn, p)
	}()
	mp.RaftPartition.AddNode(req.AddPeer.ID, req.AddPeer.Addr)
	resp := proto.MetaPartitionOfflineResponse{
		PartitionID: req.PartitionID,
		Status:      proto.OpErr,
	}
	// add peer
	reqData, err = json.Marshal(req.AddPeer)
	if err != nil {
		resp.Result = err.Error()
		goto end
	}
	_, err = mp.RaftPartition.ChangeMember(raftproto.ConfAddNode,
		raftproto.Peer{ID: req.AddPeer.ID}, reqData)
	if err != nil {
		resp.Result = err.Error()
		goto end
	}
	// delete peer
	reqData, err = json.Marshal(req.RemovePeer)
	if err != nil {
		resp.Result = err.Error()
		goto end
	}
	_, err = mp.RaftPartition.ChangeMember(raftproto.ConfRemoveNode,
		raftproto.Peer{ID: req.RemovePeer.ID}, reqData)
	if err != nil {
		resp.Result = err.Error()
		goto end
	}
	resp.Status = proto.OpOk
end:
	adminTask.Request = nil
	adminTask.Response = resp
	m.replyToMaster(m.masterAddr, adminTask)
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
	url := fmt.Sprintf("http://%s%s", ip, metaNodeResponse)
	util.PostToNode(jsonBytes, url)
	return
}

func (m *MetaNode) ackAdmin(conn net.Conn, p *Packet) {
	var err error
	err = p.WriteToConn(conn)
	if err != nil {
		log.Error("Ack Master Request: %s", err.Error())
	}
	return
}
