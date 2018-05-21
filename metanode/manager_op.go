package metanode

import (
	"encoding/json"
	"fmt"
	"net"
	"strings"

	"github.com/tiglabs/baudstorage/proto"
	"github.com/tiglabs/baudstorage/util"
	"github.com/tiglabs/baudstorage/util/log"
	raftproto "github.com/tiglabs/raft/proto"
)

func (m *metaManager) opMasterHeartbeat(conn net.Conn, p *Packet) (err error) {
	adminTask := &proto.AdminTask{}
	if err = json.Unmarshal(p.Data, adminTask); err != nil {
		p.PackErrorWithBody(proto.OpErr, nil)
		m.respondToClient(conn, p)
		return
	}
	var (
		req     = &proto.HeartBeatRequest{}
		reqData []byte
	)
	reqData, err = json.Marshal(adminTask.Request)
	if err != nil {
		p.PackErrorWithBody(proto.OpErr, nil)
		m.respondToClient(conn, p)
		return
	}
	if err = json.Unmarshal(reqData, req); err != nil {
		p.PackErrorWithBody(proto.OpErr, nil)
		m.respondToClient(conn, p)
		return
	}
	// For ack to master
	m.responseAckOKToMaster(conn, p)
	m.masterAddr = req.MasterAddr
	// collect used info
	resp := &proto.MetaNodeHeartbeatResponse{}
	// machine mem total and used
	resp.Total, resp.Used, err = util.GetMemInfo()
	if err != nil {
		adminTask.Status = proto.TaskFail
		goto end
	}
	// every partition used
	for id, partition := range m.partitions {
		mpr := &proto.MetaPartitionReport{}
		mpr.PartitionID = id
		addr, isLeader := partition.IsLeader()
		if addr != "" {
			mpr.Status = proto.CmdFailed
		}
		mpr.IsLeader = isLeader
		mpr.MaxInodeID = partition.GetCursor()
		resp.MetaPartitionInfo = append(resp.MetaPartitionInfo, mpr)
	}
	resp.Status = proto.TaskSuccess

end:
	m.respondToMaster(m.masterAddr, adminTask)
	return
}

// Handle OpCreateMetaRange
func (m *metaManager) opCreateMetaPartition(conn net.Conn, p *Packet) (err error) {
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
		adminTask.Request = nil
		m.respondToMaster(strings.Split(conn.RemoteAddr().String(), ":")[0], adminTask)
	}()
	// Marshal request body.
	requestJson, err := json.Marshal(adminTask.Request)
	if err != nil {
		return
	}
	// Unmarshal request to entity
	req := &proto.CreateMetaPartitionRequest{}
	if err = json.Unmarshal(requestJson, req); err != nil {
		return
	}
	// Create new  metaPartition.
	err = m.createPartition(req.PartitionID, req.Start, req.End, req.Members)
	return
}

// Handle OpCreate Inode
func (m *metaManager) opCreateInode(conn net.Conn, p *Packet) (err error) {
	req := &CreateInoReq{}
	if err = json.Unmarshal(p.Data, req); err != nil {
		return
	}
	mp, err := m.getPartition(req.PartitionID)
	if err != nil {
		return
	}
	if ok := m.serveProxy(conn, mp, p); !ok {
		return
	}
	if err = mp.CreateInode(req, p); err != nil {
		log.LogError(fmt.Sprintf("Create Inode Request: %s", err.Error()))
	}
	// Reply operation result to client though TCP connection.
	err = m.respondToClient(conn, p)
	return
}

// Handle OpCreate
func (m *metaManager) opCreateDentry(conn net.Conn, p *Packet) (err error) {
	req := &CreateDentryReq{}
	if err = json.Unmarshal(p.Data, req); err != nil {
		return
	}
	mp, err := m.getPartition(req.PartitionID)
	if err != nil {
		return err
	}
	if ok := m.serveProxy(conn, mp, p); !ok {
		return
	}
	err = mp.CreateDentry(req, p)
	if err != nil {
		log.LogError(fmt.Sprintf("Create Dentry: %s", err.Error()))
	}
	// Reply operation result to client though TCP connection.
	err = m.respondToClient(conn, p)
	return
}

// Handle OpDelete Dentry
func (m *metaManager) opDeleteDentry(conn net.Conn, p *Packet) (err error) {
	req := &DeleteDentryReq{}
	if err = json.Unmarshal(p.Data, req); err != nil {
		return
	}
	mp, err := m.getPartition(req.PartitionID)
	if err != nil {
		return
	}
	if ok := m.serveProxy(conn, mp, p); !ok {
		return
	}
	err = mp.DeleteDentry(req, p)
	if err != nil {
		return
	}
	// Reply operation result to client though TCP connection.
	err = m.respondToClient(conn, p)
	return
}

func (m *metaManager) opDeleteInode(conn net.Conn, p *Packet) (err error) {
	req := &DeleteInoReq{}
	if err = json.Unmarshal(p.Data, req); err != nil {
		return
	}
	mp, err := m.getPartition(req.PartitionID)
	if err != nil {
		return
	}
	if ok := m.serveProxy(conn, mp, p); !ok {
		return
	}
	err = mp.DeleteInode(req, p)
	if err != nil {
		return
	}
	err = m.respondToClient(conn, p)
	return
}

// Handle OpReadDir
func (m *metaManager) opReadDir(conn net.Conn, p *Packet) (err error) {
	req := &proto.ReadDirRequest{}
	if err = json.Unmarshal(p.Data, req); err != nil {
		return
	}
	mp, err := m.getPartition(req.PartitionID)
	if err != nil {
		return
	}
	if ok := m.serveProxy(conn, mp, p); !ok {
		return
	}
	err = mp.ReadDir(req, p)
	if err != nil {
		return
	}
	// Reply operation result to client though TCP connection.
	err = m.respondToClient(conn, p)
	return
}

// Handle OpOpen
func (m *metaManager) opOpen(conn net.Conn, p *Packet) (err error) {
	req := &proto.OpenRequest{}
	if err = json.Unmarshal(p.Data, req); err != nil {
		return
	}
	mp, err := m.getPartition(req.PartitionID)
	if err != nil {
		return
	}
	if ok := m.serveProxy(conn, mp, p); !ok {
		return
	}
	err = mp.Open(req, p)
	if err != nil {
		return
	}
	// Reply operation result to client though TCP connection.
	err = m.respondToClient(conn, p)
	return
}

func (m *metaManager) opMetaInodeGet(conn net.Conn, p *Packet) (err error) {
	req := &proto.InodeGetRequest{}
	if err = json.Unmarshal(p.Data, req); err != nil {
		return
	}
	mp, err := m.getPartition(req.PartitionID)
	if err != nil {
		return
	}
	if m.serveProxy(conn, mp, p) {
		return
	}
	if err = mp.InodeGet(req, p); err != nil {
		return
	}
	m.respondToClient(conn, p)
	return
}

func (m *metaManager) opMetaLookup(conn net.Conn, p *Packet) (err error) {
	req := &proto.LookupRequest{}
	if err = json.Unmarshal(p.Data, req); err != nil {
		return
	}
	mp, err := m.getPartition(req.PartitionID)
	if err != nil {
		return
	}
	if m.serveProxy(conn, mp, p) {
		return
	}
	if err = mp.Lookup(req, p); err != nil {
		return
	}
	m.respondToClient(conn, p)
	return
}

func (m *metaManager) opMetaExtentsAdd(conn net.Conn, p *Packet) (err error) {
	req := &proto.AppendExtentKeyRequest{}
	if err = json.Unmarshal(p.Data, req); err != nil {
		return
	}
	mp, err := m.getPartition(req.PartitionID)
	if err != nil {
		return
	}
	if m.serveProxy(conn, mp, p) {
		return
	}
	err = mp.ExtentAppend(req, p)
	m.respondToClient(conn, p)
	return
}

func (m *metaManager) opMetaExtentsList(conn net.Conn, p *Packet) (err error) {
	req := &proto.GetExtentsRequest{}
	if err = json.Unmarshal(p.Data, req); err != nil {
		return
	}
	mp, err := m.getPartition(req.PartitionID)
	if err != nil {
		return
	}
	if m.serveProxy(conn, mp, p) {
		return
	}

	if err = mp.ExtentsList(req, p); err != nil {
		//TODO: log
	}
	m.respondToClient(conn, p)
	return
}

func (m *metaManager) opMetaExtentsDel(conn net.Conn, p *Packet) (err error) {
	return
}

func (m *metaManager) opDeleteMetaPartition(conn net.Conn, p *Packet) (err error) {
	adminTask := &proto.AdminTask{}
	if err = json.Unmarshal(p.Data, adminTask); err != nil {
		p.PackErrorWithBody(proto.OpErr, nil)
		m.respondToClient(conn, p)
		return
	}
	var (
		mp      MetaPartition
		reqData []byte
	)
	req := &proto.DeleteMetaPartitionRequest{}
	reqData, err = json.Marshal(adminTask.Request)
	if err != nil {
		p.PackErrorWithBody(proto.OpErr, nil)
		m.respondToClient(conn, p)
		return
	}
	if err = json.Unmarshal(reqData, req); err != nil {
		p.PackErrorWithBody(proto.OpErr, nil)
		m.respondToClient(conn, p)
		return
	}
	resp := &proto.DeleteMetaPartitionResponse{
		PartitionID: req.PartitionID,
		Status:      proto.OpErr,
	}
	mp, err = m.getPartition(req.PartitionID)
	if err != nil {
		p.PackErrorWithBody(proto.OpErr, nil)
		m.respondToClient(conn, p)
		return
	}
	if m.serveProxy(conn, mp, p) {
		return
	}
	// Ack Master Request
	m.responseAckOKToMaster(conn, p)
	if err = mp.DeletePartition(); err != nil {
		resp.Result = err.Error()
		goto end
	}
	resp.Status = proto.OpOk
end:
	adminTask.Response = resp
	adminTask.Request = nil
	err = m.respondToMaster(m.masterAddr, adminTask)
	return
}

func (m *metaManager) opUpdateMetaPartition(conn net.Conn, p *Packet) (err error) {
	adminTask := &proto.AdminTask{}
	if err = json.Unmarshal(p.Data, adminTask); err != nil {
		p.PackErrorWithBody(proto.OpErr, nil)
		m.respondToClient(conn, p)
		return
	}
	var (
		reqData []byte
		req     = &proto.UpdateMetaPartitionRequest{}
	)
	reqData, err = json.Marshal(adminTask.Request)
	if err != nil {
		p.PackErrorWithBody(proto.OpErr, nil)
		m.respondToClient(conn, p)
		return
	}
	if err = json.Unmarshal(reqData, req); err != nil {
		p.PackErrorWithBody(proto.OpErr, nil)
		m.respondToClient(conn, p)
		return
	}
	mp, err := m.getPartition(req.PartitionID)
	if err != nil {
		p.PackErrorWithBody(proto.OpErr, nil)
		m.respondToClient(conn, p)
		return
	}
	if m.serveProxy(conn, mp, p) {
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
	err = m.respondToMaster(m.masterAddr, adminTask)
	return
}

func (m *metaManager) opLoadMetaPartition(conn net.Conn, p *Packet) (err error) {
	adminTask := &proto.AdminTask{}
	if err = json.Unmarshal(p.Data, adminTask); err != nil {
		p.PackErrorWithBody(proto.OpErr, nil)
		p.WriteToConn(conn)
		return
	}
	var (
		req     = &proto.LoadMetaPartitionMetricRequest{}
		resp    = &proto.LoadMetaPartitionMetricResponse{}
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
	mp, err := m.getPartition(req.PartitionID)
	if err != nil {
		resp.Status = proto.OpErr
		resp.Result = err.Error()
		adminTask.Response = resp
		adminTask.Request = nil
		m.respondToMaster(m.masterAddr, adminTask)
		return
	}
	mConf := mp.GetBaseConfig()
	resp.Start = mConf.Start
	resp.End = mConf.End
	resp.MaxInode = mConf.Cursor
	resp.Status = proto.OpOk
	adminTask.Response = resp
	adminTask.Request = nil
	m.respondToMaster(m.masterAddr, adminTask)
	return
}

func (m *metaManager) opOfflineMetaPartition(conn net.Conn, p *Packet) (err error) {
	var (
		reqData []byte
		req     = &proto.MetaPartitionOfflineRequest{}
	)
	adminTask := &proto.AdminTask{}
	if err = json.Unmarshal(p.Data, adminTask); err != nil {
		p.PackErrorWithBody(proto.OpErr, nil)
		m.respondToClient(conn, p)
		return
	}
	reqData, err = json.Marshal(adminTask.Request)
	if err != nil {
		p.PackErrorWithBody(proto.OpErr, nil)
		m.respondToClient(conn, p)
		return
	}
	if err = json.Unmarshal(reqData, req); err != nil {
		p.PackErrorWithBody(proto.OpErr, nil)
		m.respondToClient(conn, p)
		return
	}
	mp, err := m.getPartition(req.PartitionID)
	if err != nil {
		p.PackErrorWithBody(proto.OpErr, nil)
		m.respondToClient(conn, p)
		return
	}
	if m.serveProxy(conn, mp, p) {
		return
	}
	defer func() {
		p.PackOkReply()
		m.respondToClient(conn, p)
	}()
	m.raftStore.AddNode(req.AddPeer.ID, req.AddPeer.Addr)
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
	_, err = mp.ChangeMember(raftproto.ConfAddNode,
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
	_, err = mp.ChangeMember(raftproto.ConfRemoveNode,
		raftproto.Peer{ID: req.RemovePeer.ID}, reqData)
	if err != nil {
		resp.Result = err.Error()
		goto end
	}
	resp.Status = proto.OpOk
end:
	adminTask.Request = nil
	adminTask.Response = resp
	m.respondToMaster(m.masterAddr, adminTask)
	return
}
