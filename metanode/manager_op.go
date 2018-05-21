package metanode

import (
	"encoding/json"
	"fmt"
	"github.com/tiglabs/baudstorage/proto"
	"github.com/tiglabs/baudstorage/util"
	"github.com/tiglabs/baudstorage/util/log"
	"net"
	"strconv"
	"strings"
)

func (m *metaManager) opMasterHeartbeat(conn net.Conn, p *Packet) (err error) {
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
		for id, partition := range m.partitions {
			mpr := &proto.MetaPartitionReport{}
			mpr.PartitionID, _ = strconv.ParseUint(id, 10, 8)
			_, mpr.IsLeader = partition.IsLeader()
			mpr.Status = 1
			resp.MetaPartitionInfo = append(resp.MetaPartitionInfo, mpr)
		}
		resp.Status = proto.OpOk
	}

end:
	err = m.respondToMaster(m.masterAddr, adminTask)
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
	id := strconv.FormatUint(req.PartitionID, 10)
	err = m.createPartition(id, req.Start, req.End, req.Members)
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

func (m *MetaNode) opMetaExtentsDel(conn net.Conn, p *Packet) (err error) {
	return
}
