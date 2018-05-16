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
)

const (
	// Operation response
	metaNodeResponse = "metaNode/response" // Method: 'POST', ContentType: 'application/json'
)

// Handle OpCreateMetaRange
func (m *MetaNode) opCreateMetaRange(conn net.Conn, p *Packet) (err error) {
	// Ack to master
	go m.ackAdmin(conn, p)
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
		m.replyToMaster(m.masterAddr, adminTask)
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
	// Create new  MetaPartition.
	id := fmt.Sprintf("%d", req.GroupId)
	mConf := MetaPartitionConfig{
		ID:          id,
		Start:       req.Start,
		End:         req.End,
		Cursor:      req.Start,
		RaftGroupID: req.GroupId,
		Peers:       req.Members,
		RootDir:     path.Join(m.metaDir, id),
	}
	mr := NewMetaPartition(mConf)
	if err = m.metaManager.SetMetaRange(mr); err != nil {
		return
	}
	defer func() {
		if err != nil {
			m.metaManager.DeleteMetaRange(mr.ID)
		}
	}()
	// Create metaPartition base dir
	if _, err = os.Stat(mr.RootDir); err == nil {
		err = errors.New(fmt.Sprint("metaPartition root dir '%s' is exsited!",
			mr.RootDir))
		return
	}
	os.MkdirAll(mr.RootDir, 0755)
	defer func() {
		if err != nil {
			os.RemoveAll(mr.RootDir)
		}
	}()
	// Write metaPartition to file
	if err = mr.StoreMeta(); err != nil {
		return
	}
	//TODO: create Raft
	if err = m.createPartition(mr); err != nil {
		return
	}
	go mr.StartStoreSchedule()
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
	{
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
			mpr.GroupId = mp.RaftGroupID
			mpr.IsLeader = mp.RaftPartition.IsLeader()
			mpr.Status = 1
			mpr.Used = mp.Sizeof()
			resp.MetaPartitionInfo = append(resp.MetaPartitionInfo, mpr)
			return true
		})
		resp.Status = proto.OpOk
	}

end:
	if m.masterAddr == "" {
		err = ErrNotLeader
		return
	}
	err = m.replyToMaster(m.masterAddr, adminTask)
	return
}

func (m *MetaNode) opDeleteMetaPartition(conn net.Conn, p *Packet) (err error) {
	adminTask := &proto.AdminTask{}
	if err = json.Unmarshal(p.Data, adminTask); err != nil {
		adminTask.Status = proto.TaskFail
		goto end
	}
	if true {
		var (
			mp      *MetaPartition
			reqData []byte
		)
		req := &proto.DeleteMetaPartitionRequest{}
		resp := &proto.DeleteMetaPartitionResponse{}
		reqData, err = json.Marshal(adminTask.Request)
		if err != nil {
			adminTask.Status = proto.TaskFail
			goto end
		}
		if err = json.Unmarshal(reqData, req); err != nil {
			adminTask.Status = proto.TaskFail
			goto end
		}
		mp, err = m.metaManager.LoadMetaPartition(fmt.Sprintf("%d", req.GroupId))
		if err != nil {
			adminTask.Status = proto.TaskFail
			goto end
		}
		mp.Stop()
		m.metaManager.DeleteMetaRange(fmt.Sprintf("%d", req.GroupId))
		resp.GroupId = mp.RaftGroupID
		resp.Status = 1
		adminTask.Response = resp
	}
end:
	if m.masterAddr == "" {
		err = ErrNotLeader
		return
	}
	err = m.replyToMaster(m.masterAddr, adminTask)
	return
}

func (m *MetaNode) opUpdateMetaPartition(conn net.Conn, p *Packet) (err error) {
	return
}

func (m *MetaNode) opLoadMetaPartition(conn net.Conn, p *Packet) (err error) {
	adminTask := &proto.AdminTask{}
	if err = json.Unmarshal(p.Data, adminTask); err != nil {
		adminTask.Status = proto.TaskFail
		goto end
	}
	// parse req
	{
		var (
			req     = &proto.LoadMetaPartitionMetricRequest{}
			resp    = &proto.LoadMetaPartitionMetricResponse{}
			mp      *MetaPartition
			reqData []byte
		)
		if reqData, err = json.Marshal(adminTask.Request); err != nil {
			adminTask.Status = proto.TaskFail
			goto end
		}
		if err = json.Unmarshal(reqData, req); err != nil {
			adminTask.Status = proto.TaskFail
			goto end
		}
		mp, err = m.metaManager.LoadMetaPartition(fmt.Sprintf("%d",
			req.PartitionID))
		if err != nil {
			adminTask.Status = proto.TaskFail
			goto end
		}
		resp.Start = mp.Start
		resp.End = mp.End
		resp.MaxInode = mp.Cursor
		resp.Status = 1
		adminTask.Response = resp
	}
end:
	if m.masterAddr == "" {
		err = ErrNotLeader
		return
	}
	err = m.replyToMaster(m.masterAddr, adminTask)
	return
}

func (m *MetaNode) opOfflineMetaPartition(conn net.Conn, p *Packet) (err error) {
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

func (m *MetaNode) ackAdmin(conn net.Conn, p *Packet) (err error) {
	p.PackOkReply()
	err = p.WriteToConn(conn)
	return
}
