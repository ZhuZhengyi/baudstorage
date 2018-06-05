package metanode

import (
	"encoding/json"
	"net"

	"github.com/juju/errors"
	"github.com/tiglabs/baudstorage/proto"
	"github.com/tiglabs/baudstorage/util"
	"github.com/tiglabs/baudstorage/util/log"
	raftProto "github.com/tiglabs/raft/proto"
	"os"
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
	if curMasterAddr != req.MasterAddr {
		curMasterAddr = req.MasterAddr
	}
	// collect used info
	resp := &proto.MetaNodeHeartbeatResponse{}
	// machine mem total and used
	resp.Total, resp.Used, err = util.GetMemInfo()
	if err != nil {
		adminTask.Status = proto.TaskFail
		goto end
	}
	// every partition used
	m.Range(func(id uint64, partition MetaPartition) bool {
		mConf := partition.GetBaseConfig()
		mpr := &proto.MetaPartitionReport{
			PartitionID: mConf.PartitionId,
			Start:       mConf.Start,
			End:         mConf.End,
			Status:      proto.TaskSuccess,
			MaxInodeID:  mConf.Cursor,
		}
		addr, isLeader := partition.IsLeader()
		if addr == "" {
			mpr.Status = proto.TaskFail
		}
		mpr.IsLeader = isLeader
		resp.MetaPartitionInfo = append(resp.MetaPartitionInfo, mpr)
		return true
	})
	resp.Status = proto.TaskSuccess
	adminTask.Request = nil
	adminTask.Response = resp
end:
	m.respondToMaster(adminTask)
	return
}

// Handle OpCreateMetaRange
func (m *metaManager) opCreateMetaPartition(conn net.Conn, p *Packet) (err error) {
	// Get task from packet.
	adminTask := &proto.AdminTask{}
	if err = json.Unmarshal(p.Data, adminTask); err != nil {
		p.PackErrorWithBody(proto.OpErr, nil)
		m.respondToClient(conn, p)
		err = errors.Errorf("[opCreateMetaPartition]: Unmarshal AdminTask"+
			" struct: %s", err.Error())
		return
	}
	log.LogDebugf("[opCreateMetaPartition] [remoteAddr=%s]accept a from"+
		" master message: %v", conn.RemoteAddr(), adminTask)
	// Marshal request body.
	requestJson, err := json.Marshal(adminTask.Request)
	if err != nil {
		p.PackErrorWithBody(proto.OpErr, nil)
		m.respondToClient(conn, p)
		err = errors.Errorf("[opCreateMetaPartition]: Marshal AdminTask."+
			"Request: %s", err.Error())
		return
	}
	// Unmarshal request to entity
	req := &proto.CreateMetaPartitionRequest{}
	if err = json.Unmarshal(requestJson, req); err != nil {
		p.PackErrorWithBody(proto.OpErr, nil)
		m.respondToClient(conn, p)
		err = errors.Errorf("[opCreateMetaPartition]: Unmarshal AdminTask."+
			"Request to CreateMetaPartitionRequest: %s", err.Error())
		return
	}
	m.responseAckOKToMaster(conn, p)
	adminTask.Request = nil
	resp := proto.CreateMetaPartitionResponse{
		NsName:      req.NsName,
		PartitionID: req.PartitionID,
		Status:      proto.TaskSuccess,
	}
	// Create new  metaPartition.
	if err = m.createPartition(req.PartitionID, req.NsName, req.Start, req.End,
		req.Members); err != nil {
		resp.Status = proto.TaskFail
		resp.Result = err.Error()
		err = errors.Errorf("[opCreateMetaPartition]->%s; request message: %v",
			err.Error(), adminTask.Request)
	}
	adminTask.Response = resp
	m.respondToMaster(adminTask)
	return
}

// Handle OpCreate Inode
func (m *metaManager) opCreateInode(conn net.Conn, p *Packet) (err error) {
	req := &CreateInoReq{}
	if err = json.Unmarshal(p.Data, req); err != nil {
		p.PackErrorWithBody(proto.OpErr, nil)
		m.respondToClient(conn, p)
		return
	}
	mp, err := m.getPartition(req.PartitionID)
	if err != nil {
		p.PackErrorWithBody(proto.OpNotExistErr, nil)
		m.respondToClient(conn, p)
		return
	}
	if !m.serveProxy(conn, mp, p) {
		return
	}
	err = mp.CreateInode(req, p)
	// Reply operation result to client though TCP connection.
	m.respondToClient(conn, p)
	return
}

// Handle OpCreate
func (m *metaManager) opCreateDentry(conn net.Conn, p *Packet) (err error) {
	req := &CreateDentryReq{}
	if err = json.Unmarshal(p.Data, req); err != nil {
		p.PackErrorWithBody(proto.OpErr, nil)
		m.respondToClient(conn, p)
		return
	}
	mp, err := m.getPartition(req.PartitionID)
	if err != nil {
		p.PackErrorWithBody(proto.OpNotExistErr, nil)
		m.respondToClient(conn, p)
		return
	}
	if !m.serveProxy(conn, mp, p) {
		return
	}
	err = mp.CreateDentry(req, p)
	// Reply operation result to client though TCP connection.
	m.respondToClient(conn, p)
	return
}

// Handle OpDelete Dentry
func (m *metaManager) opDeleteDentry(conn net.Conn, p *Packet) (err error) {
	req := &DeleteDentryReq{}
	if err = json.Unmarshal(p.Data, req); err != nil {
		p.PackErrorWithBody(proto.OpErr, nil)
		m.respondToClient(conn, p)
		return
	}
	mp, err := m.getPartition(req.PartitionID)
	if err != nil {
		p.PackErrorWithBody(proto.OpNotExistErr, nil)
		m.respondToClient(conn, p)
		return
	}
	if !m.serveProxy(conn, mp, p) {
		return
	}
	err = mp.DeleteDentry(req, p)
	// Reply operation result to client though TCP connection.
	m.respondToClient(conn, p)
	return
}

func (m *metaManager) opDeleteInode(conn net.Conn, p *Packet) (err error) {
	req := &DeleteInoReq{}
	if err = json.Unmarshal(p.Data, req); err != nil {
		p.PackErrorWithBody(proto.OpErr, nil)
		m.respondToClient(conn, p)
		return
	}
	mp, err := m.getPartition(req.PartitionID)
	if err != nil {
		p.PackErrorWithBody(proto.OpNotExistErr, nil)
		m.respondToClient(conn, p)
		return
	}
	if !m.serveProxy(conn, mp, p) {
		return
	}
	err = mp.DeleteInode(req, p)
	m.respondToClient(conn, p)
	return
}

// Handle OpReadDir
func (m *metaManager) opReadDir(conn net.Conn, p *Packet) (err error) {
	req := &proto.ReadDirRequest{}
	if err = json.Unmarshal(p.Data, req); err != nil {
		p.PackErrorWithBody(proto.OpErr, nil)
		m.respondToClient(conn, p)
		return
	}
	mp, err := m.getPartition(req.PartitionID)
	if err != nil {
		p.PackErrorWithBody(proto.OpNotExistErr, nil)
		m.respondToClient(conn, p)
		return
	}
	if !m.serveProxy(conn, mp, p) {
		return
	}
	err = mp.ReadDir(req, p)
	// Reply operation result to client though TCP connection.
	m.respondToClient(conn, p)
	return
}

// Handle OpOpen
func (m *metaManager) opOpen(conn net.Conn, p *Packet) (err error) {
	req := &proto.OpenRequest{}
	if err = json.Unmarshal(p.Data, req); err != nil {
		p.PackErrorWithBody(proto.OpErr, nil)
		m.respondToClient(conn, p)
		return
	}
	mp, err := m.getPartition(req.PartitionID)
	if err != nil {
		p.PackErrorWithBody(proto.OpNotExistErr, nil)
		m.respondToClient(conn, p)
		return
	}
	if ok := m.serveProxy(conn, mp, p); !ok {
		return
	}
	err = mp.Open(req, p)
	// Reply operation result to client though TCP connection.
	m.respondToClient(conn, p)
	return
}

func (m *metaManager) opMetaInodeGet(conn net.Conn, p *Packet) (err error) {
	req := &InodeGetReq{}
	if err = json.Unmarshal(p.Data, req); err != nil {
		p.PackErrorWithBody(proto.OpErr, nil)
		m.respondToClient(conn, p)
		err = errors.Errorf("[opMetaInodeGet]: %s", err.Error())
		return
	}
	mp, err := m.getPartition(req.PartitionID)
	if err != nil {
		p.PackErrorWithBody(proto.OpNotExistErr, nil)
		m.respondToClient(conn, p)
		err = errors.Errorf("[opMetaInodeGet] %s", err.Error())
		return
	}
	if !m.serveProxy(conn, mp, p) {
		return
	}
	if err = mp.InodeGet(req, p); err != nil {
		err = errors.Errorf("[opMetaInodeGet] %s", err.Error())
	}
	m.respondToClient(conn, p)
	return
}

func (m *metaManager) opMetaLookup(conn net.Conn, p *Packet) (err error) {
	req := &proto.LookupRequest{}
	if err = json.Unmarshal(p.Data, req); err != nil {
		p.PackErrorWithBody(proto.OpErr, nil)
		m.respondToClient(conn, p)
		return
	}
	mp, err := m.getPartition(req.PartitionID)
	if err != nil {
		p.PackErrorWithBody(proto.OpNotExistErr, nil)
		m.respondToClient(conn, p)
		return
	}
	if !m.serveProxy(conn, mp, p) {
		return
	}
	err = mp.Lookup(req, p)
	m.respondToClient(conn, p)
	return
}

func (m *metaManager) opMetaExtentsAdd(conn net.Conn, p *Packet) (err error) {
	req := &proto.AppendExtentKeyRequest{}
	if err = json.Unmarshal(p.Data, req); err != nil {
		p.PackErrorWithBody(proto.OpErr, nil)
		m.respondToClient(conn, p)
		return
	}
	mp, err := m.getPartition(req.PartitionID)
	if err != nil {
		p.PackErrorWithBody(proto.OpNotExistErr, nil)
		m.respondToClient(conn, p)
		return
	}
	if !m.serveProxy(conn, mp, p) {
		return
	}
	err = mp.ExtentAppend(req, p)
	m.respondToClient(conn, p)
	return
}

func (m *metaManager) opMetaExtentsList(conn net.Conn, p *Packet) (err error) {
	req := &proto.GetExtentsRequest{}
	if err = json.Unmarshal(p.Data, req); err != nil {
		p.PackErrorWithBody(proto.OpErr, nil)
		m.respondToClient(conn, p)
		return
	}
	mp, err := m.getPartition(req.PartitionID)
	if err != nil {
		p.PackErrorWithBody(proto.OpNotExistErr, nil)
		m.respondToClient(conn, p)
		return
	}
	if !m.serveProxy(conn, mp, p) {
		return
	}

	err = mp.ExtentsList(req, p)
	m.respondToClient(conn, p)
	return
}

func (m *metaManager) opMetaExtentsDel(conn net.Conn, p *Packet) (err error) {
	// TODO: not implement yet
	panic("not implement yet")
}

func (m *metaManager) opDeleteMetaPartition(conn net.Conn, p *Packet) (err error) {
	adminTask := &proto.AdminTask{}
	if err = json.Unmarshal(p.Data, adminTask); err != nil {
		p.PackErrorWithBody(proto.OpErr, nil)
		m.respondToClient(conn, p)
		return
	}
	log.LogDebugf("[opDeleteMetaPartition]: received %v", adminTask)
	req := &proto.DeleteMetaPartitionRequest{}
	reqData, err := json.Marshal(adminTask.Request)
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
		p.PackErrorWithBody(proto.OpNotExistErr, nil)
		m.respondToClient(conn, p)
		return
	}
	resp := &proto.DeleteMetaPartitionResponse{
		PartitionID: req.PartitionID,
		Status:      proto.TaskSuccess,
	}
	// Ack Master Request
	m.responseAckOKToMaster(conn, p)
	conf := mp.GetBaseConfig()
	mp.Stop()
	err = mp.DeleteRaft()
	os.RemoveAll(conf.RootDir)
	if err != nil {
		resp.Status = proto.TaskFail
	}
	adminTask.Response = resp
	adminTask.Request = nil
	err = m.respondToMaster(adminTask)
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
	if !m.serveProxy(conn, mp, p) {
		return
	}
	m.responseAckOKToMaster(conn, p)
	resp := &proto.UpdateMetaPartitionResponse{
		NsName:      req.NsName,
		PartitionID: req.PartitionID,
		End:         req.End,
		Status:      proto.OpOk,
	}
	err = mp.UpdatePartition(req, resp)
	adminTask.Response = resp
	adminTask.Request = nil
	m.respondToMaster(adminTask)
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
	m.responseAckOKToMaster(conn, p)
	mp, err := m.getPartition(req.PartitionID)
	if err != nil {
		resp.Status = proto.OpErr
		resp.Result = err.Error()
		adminTask.Response = resp
		adminTask.Request = nil
		m.respondToMaster(adminTask)
		return
	}
	mConf := mp.GetBaseConfig()
	resp.Start = mConf.Start
	resp.End = mConf.End
	resp.MaxInode = mConf.Cursor
	resp.Status = proto.OpOk
	adminTask.Response = resp
	adminTask.Request = nil
	m.respondToMaster(adminTask)
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
	log.LogDebugf("[opOfflineMetaPartition] received task: %v", adminTask)
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
	if !m.serveProxy(conn, mp, p) {
		return
	}
	m.responseAckOKToMaster(conn, p)
	resp := proto.MetaPartitionOfflineResponse{
		PartitionID: req.PartitionID,
		NsName:      req.NsName,
		Status:      proto.TaskFail,
	}
	if req.AddPeer.ID == req.RemovePeer.ID {
		err = errors.Errorf("[opOfflineMetaPartition]: AddPeer[%v] same withRemovePeer[%v]", req.AddPeer, req.RemovePeer)
		resp.Result = err.Error()
		goto end
	}
	_, err = mp.ChangeMember(raftProto.ConfAddNode,
		raftProto.Peer{ID: req.AddPeer.ID}, reqData)
	if err != nil {
		resp.Result = err.Error()
		goto end
	}
	_, err = mp.ChangeMember(raftProto.ConfRemoveNode,
		raftProto.Peer{ID: req.RemovePeer.ID}, reqData)
	if err != nil {
		resp.Result = err.Error()
		goto end
	}
	{
		conf := mp.GetBaseConfig()
		if req.RemovePeer.ID == conf.NodeId {
			mp.Stop()
			os.RemoveAll(conf.RootDir)
			log.LogDebugf("[opOfflineMetaPartition]: delete self!")
		}
	}
	resp.Status = proto.TaskSuccess
end:
	adminTask.Request = nil
	adminTask.Response = resp
	m.respondToMaster(adminTask)
	log.LogDebugf("[opOfflineMetaPartition]: the end %v", adminTask)
	return
}
