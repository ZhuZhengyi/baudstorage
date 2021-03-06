package datanode

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"github.com/juju/errors"
	"github.com/tiglabs/baudstorage/master"
	"github.com/tiglabs/baudstorage/proto"
	"github.com/tiglabs/baudstorage/storage"
	"github.com/tiglabs/baudstorage/util"
	"github.com/tiglabs/baudstorage/util/log"
	"github.com/tiglabs/baudstorage/util/ump"
	"net"
	"strconv"
	"time"
)

var (
	ErrorUnknownOp = errors.New("unknown opcode")
)

func (s *DataNode) operatePacket(pkg *Packet, c *net.TCPConn) {
	orgSize := pkg.Size
	umpKey := fmt.Sprintf("%s_datanode_%s", s.clusterId, pkg.GetOpMsg())
	tpObject := ump.BeforeTP(umpKey)
	start := time.Now().UnixNano()
	var err error
	defer func() {
		resultSize := pkg.Size
		pkg.Size = orgSize
		if pkg.IsErrPack() {
			err = fmt.Errorf("operation[%v] error[%v]", pkg.GetOpMsg(), string(pkg.Data[:resultSize]))
			log.LogErrorf("action[DataNode.operatePacket] %v", err)
		} else if !pkg.IsMasterCommand() {
			if pkg.IsReadReq() {
				log.LogReadf("action[DataNode.operatePacket] %dp.",
					pkg.ActionMsg(pkg.GetOpMsg(), LocalProcessAddr, start, nil))
			} else {
				log.LogWritef("action[DataNode.operatePacket] %dp.",
					pkg.ActionMsg(pkg.GetOpMsg(), LocalProcessAddr, start, nil))
			}
		}
		pkg.Size = resultSize
		ump.AfterTP(tpObject, err)
	}()
	switch pkg.Opcode {
	case proto.OpCreateFile:
		s.createFile(pkg)
	case proto.OpWrite:
		s.append(pkg)
	case proto.OpRead:
		s.read(pkg)
	case proto.OpCRepairRead:
		s.repairObjectRead(pkg, c)
	case proto.OpSyncDelNeedle:
		s.applyDelObjects(pkg)
	case proto.OpStreamRead:
		s.streamRead(pkg, c)
	case proto.OpMarkDelete:
		s.markDel(pkg)
	case proto.OpNotifyCompact:
		s.compactChunk(pkg)
	case proto.OpNotifyRepair:
		s.repair(pkg)
	case proto.OpGetWatermark:
		s.getWatermark(pkg)
	case proto.OpGetAllWatermark:
		s.getAllWatermark(pkg)
	case proto.OpCreateDataPartition:
		s.createDataPartition(pkg)
	case proto.OpLoadDataPartition:
		s.loadDataPartition(pkg)
	case proto.OpDeleteDataPartition:
		s.deleteDataPartition(pkg)
	case proto.OpDataNodeHeartbeat:
		s.heartBeats(pkg)
	default:
		pkg.PackErrorBody(ErrorUnknownOp.Error(), ErrorUnknownOp.Error()+strconv.Itoa(int(pkg.Opcode)))
	}

	return
}

func (s *DataNode) createFile(pkg *Packet) {
	var err error
	switch pkg.StoreMode {
	case proto.TinyStoreMode:
		err = errors.Annotatef(ErrStoreTypeMismatch, " CreateFile only support ExtentMode DataPartition")
	case proto.ExtentStoreMode:
		err = pkg.dataPartition.store.(*storage.ExtentStore).Create(pkg.FileID)
	}
	if err != nil {
		err = errors.Annotatef(err, "Request[%v] CreateFile Error", pkg.GetUniqLogId())
		pkg.PackErrorBody(LogCreateFile, err.Error())
	} else {
		pkg.PackOkReply()
	}

	return
}

func (s *DataNode) createDataPartition(pkg *Packet) {
	task := &proto.AdminTask{}
	json.Unmarshal(pkg.Data, task)
	pkg.PackOkReply()
	response := &proto.CreateDataPartitionResponse{}
	request := &proto.CreateDataPartitionRequest{}
	if task.OpCode == proto.OpCreateDataPartition {
		bytes, _ := json.Marshal(task.Request)
		json.Unmarshal(bytes, request)
		_, err := s.space.chooseDiskAndCreateVol(uint32(request.PartitionId), request.PartitionType, request.PartitionSize)
		if err != nil {
			response.PartitionId = uint64(request.PartitionId)
			response.Status = proto.TaskFail
			response.Result = err.Error()
			log.LogErrorf("from master Task[%v] failed,error[%v]", task.ToString(), err.Error())
		} else {
			response.Status = proto.TaskSuccess
			response.PartitionId = request.PartitionId
		}
	} else {
		response.PartitionId = uint64(request.PartitionId)
		response.Status = proto.TaskFail
		response.Result = "illegal opcode "
		log.LogErrorf("from master Task[%v] failed,error[%v]", task.ToString(), response.Result)
	}
	task.Response = response
	data, _ := json.Marshal(task)
	_, err := PostToMaster(data, master.DataNodeResponse)
	if err != nil {
		err = errors.Annotatef(err, "create dataPartition failed,partitionId[%v]", request.PartitionId)
		log.LogError(errors.ErrorStack(err))
	}
}

func (s *DataNode) heartBeats(pkg *Packet) {
	var err error
	task := &proto.AdminTask{}
	json.Unmarshal(pkg.Data, task)
	pkg.PackOkReply()

	request := &proto.HeartBeatRequest{}
	response := &proto.DataNodeHeartBeatResponse{}

	s.fillHeartBeatResponse(response)

	if task.OpCode == proto.OpDataNodeHeartbeat {
		bytes, _ := json.Marshal(task.Request)
		json.Unmarshal(bytes, request)
		response.Status = proto.TaskSuccess
		CurrMaster = request.MasterAddr
	} else {
		response.Status = proto.TaskFail
		response.Result = "illegal opcode "
	}
	task.Response = response
	data, _ := json.Marshal(task)
	_, err = PostToMaster(data, master.DataNodeResponse)
	if err != nil {
		err = errors.Annotatef(err, "heartbeat to master[%v] failed", request.MasterAddr)
		log.LogError(errors.ErrorStack(err))
	}
}

func (s *DataNode) deleteDataPartition(pkg *Packet) {
	task := &proto.AdminTask{}
	json.Unmarshal(pkg.Data, task)
	pkg.PackOkReply()
	request := &proto.DeleteDataPartitionRequest{}
	response := &proto.DeleteDataPartitionResponse{}
	if task.OpCode == proto.OpDeleteDataPartition {
		bytes, _ := json.Marshal(task.Request)
		json.Unmarshal(bytes, request)
		_, err := s.space.chooseDiskAndCreateVol(uint32(request.PartitionId), request.DataPartitionType, request.PartitionSize)
		if err != nil {
			response.PartitionId = uint64(request.PartitionId)
			response.Status = proto.TaskFail
			response.Result = err.Error()
			log.LogErrorf("from master Task[%v] failed,error[%v]", task.ToString(), err.Error())
		} else {
			response.PartitionId = uint64(request.PartitionId)
			response.Status = proto.TaskSuccess
		}
	} else {
		response.PartitionId = uint64(request.PartitionId)
		response.Status = proto.TaskFail
		response.Result = "illegal opcode "
		log.LogErrorf("from master Task[%v] failed,error[%v]", task.ToString(), response.Result)
	}
	task.Response = response
	data, _ := json.Marshal(task)
	_, err := PostToMaster(data, master.DataNodeResponse)
	if err != nil {
		err = errors.Annotatef(err, "delete dataPartition failed,partitionId[%v]", request.PartitionId)
		log.LogError(errors.ErrorStack(err))
	}
}

func (s *DataNode) loadDataPartition(pkg *Packet) {
	task := &proto.AdminTask{}
	json.Unmarshal(pkg.Data, task)
	pkg.PackOkReply()
	request := &proto.LoadDataPartitionRequest{}
	response := &proto.LoadDataPartitionResponse{}
	if task.OpCode == proto.OpLoadDataPartition {
		bytes, _ := json.Marshal(task.Request)
		json.Unmarshal(bytes, request)
		dp := s.space.getDataPartition(uint32(request.PartitionId))
		if dp == nil {
			response.Status = proto.TaskFail
			response.PartitionId = uint64(request.PartitionId)
			response.Result = fmt.Sprintf("dataPartition[%v] not found", request.PartitionId)
			log.LogErrorf("from master Task[%v] failed,error[%v]", task.ToString(), response.Result)
		} else {
			response = dp.Load()
			response.PartitionId = uint64(request.PartitionId)
		}
	} else {
		response.PartitionId = uint64(request.PartitionId)
		response.Status = proto.TaskFail
		response.Result = "illegal opcode "
		log.LogErrorf("from master Task[%v] failed,error[%v]", task.ToString(), response.Result)
	}
	task.Response = response
	data, _ := json.Marshal(task)
	_, err := PostToMaster(data, master.DataNodeResponse)
	if err != nil {
		err = errors.Annotatef(err, "load dataPartition failed,partitionId[%v]", request.PartitionId)
		log.LogError(errors.ErrorStack(err))
	}
}

func (s *DataNode) markDel(pkg *Packet) {
	var err error
	switch pkg.StoreMode {
	case proto.TinyStoreMode:
		err = pkg.dataPartition.store.(*storage.TinyStore).MarkDelete(uint32(pkg.FileID), pkg.Offset, int64(pkg.Size))
	case proto.ExtentStoreMode:
		err = pkg.dataPartition.store.(*storage.ExtentStore).MarkDelete(pkg.FileID, pkg.Offset, int64(pkg.Size))
	}
	if err != nil {
		err = errors.Annotatef(err, "Request[%v] MarkDelete Error", pkg.GetUniqLogId())
		pkg.PackErrorBody(LogMarkDel, err.Error())
	} else {
		pkg.PackOkReply()
	}

	return
}

func (s *DataNode) append(pkg *Packet) {
	var err error
	switch pkg.StoreMode {
	case proto.TinyStoreMode:
		err = pkg.dataPartition.store.(*storage.TinyStore).Write(uint32(pkg.FileID), pkg.Offset, int64(pkg.Size), pkg.Data, pkg.Crc)
		s.AddDiskErrs(pkg.PartitionID, err, WriteFlag)
	case proto.ExtentStoreMode:
		err = pkg.dataPartition.store.(*storage.ExtentStore).Write(pkg.FileID, pkg.Offset, int64(pkg.Size), pkg.Data, pkg.Crc)
		s.AddDiskErrs(pkg.PartitionID, err, WriteFlag)
	}
	if err != nil {
		err = errors.Annotatef(err, "Request[%v] Write Error", pkg.GetUniqLogId())
		pkg.PackErrorBody(LogWrite, err.Error())
	} else {
		pkg.PackOkReply()
	}

	return
}

func (s *DataNode) read(pkg *Packet) {
	pkg.Data = make([]byte, pkg.Size)
	var err error
	pkg.Crc, err = pkg.dataPartition.store.(*storage.TinyStore).Read(uint32(pkg.FileID), pkg.Offset, int64(pkg.Size), pkg.Data)
	if err != nil {
		err = errors.Annotatef(err, "Request[%v] Read Error", pkg.GetUniqLogId())
		s.AddDiskErrs(pkg.PartitionID, err, ReadFlag)
	}
	if err == nil {
		pkg.PackOkReadReply()
	} else {
		pkg.PackErrorBody(LogRead, err.Error())
	}

	return
}

func (s *DataNode) applyDelObjects(pkg *Packet) {
	if pkg.Size%storage.ObjectIdLen != 0 {
		err := errors.Annotatef(fmt.Errorf("unvalid objectLen for opsync delete object"),
			"Request[%v] ApplyDelObjects Error", pkg.GetUniqLogId())
		pkg.PackErrorBody(LogRepairNeedles, err.Error())
		return
	}
	needles := make([]uint64, 0)
	for i := 0; i < int(pkg.Size/storage.ObjectIdLen); i++ {
		needle := binary.BigEndian.Uint64(pkg.Data[i*storage.ObjectIdLen : (i+1)*storage.ObjectIdLen])
		needles = append(needles, needle)
	}
	if err := pkg.dataPartition.store.(*storage.TinyStore).ApplyDelObjects(uint32(pkg.FileID), needles); err != nil {
		err = errors.Annotatef(err, "Request[%v] ApplyDelObjects Error", pkg.GetUniqLogId())
		pkg.PackErrorBody(LogRepair, err.Error())
		return
	}
	pkg.PackOkReply()
	return
}

func (s *DataNode) streamRead(request *Packet, connect net.Conn) {
	var (
		err error
	)
	needReplySize := request.Size
	offset := request.Offset
	store := request.dataPartition.store.(*storage.ExtentStore)
	for {
		if needReplySize <= 0 {
			break
		}
		err = nil
		currReadSize := uint32(util.Min(int(needReplySize), storage.BlockSize))
		request.Data = make([]byte, currReadSize)
		request.Crc, err = store.Read(request.FileID, offset, int64(currReadSize), request.Data)
		if err != nil {
			request.PackErrorBody(ActionStreamRead, err.Error())
			if err = request.WriteToConn(connect); err != nil {
				err = fmt.Errorf(request.ActionMsg(ActionWriteToCli, connect.RemoteAddr().String(),
					request.StartT, err))
				log.LogErrorf(err.Error())
			}
			return
		}
		request.Size = currReadSize
		request.ResultCode = proto.OpOk
		if err = request.WriteToConn(connect); err != nil {
			err = fmt.Errorf(request.ActionMsg(ActionWriteToCli, connect.RemoteAddr().String(),
				request.StartT, err))
			log.LogErrorf(err.Error())
			return
		}
		needReplySize -= currReadSize
		offset += int64(currReadSize)
		log.LogDebugf("action[DataNode.streamRead] %dp.", request.ActionMsg(ActionWriteToCli, connect.RemoteAddr().String(),
			request.StartT, err))
	}
	return
}

func (s *DataNode) getWatermark(pkg *Packet) {
	var buf []byte
	var (
		fInfo *storage.FileInfo
		err   error
	)
	switch pkg.StoreMode {
	case proto.TinyStoreMode:
		fInfo, err = pkg.dataPartition.store.(*storage.TinyStore).GetWatermark(pkg.FileID)
	case proto.ExtentStoreMode:
		fInfo, err = pkg.dataPartition.store.(*storage.ExtentStore).GetWatermark(pkg.FileID)
	}
	if err != nil {
		err = errors.Annotatef(err, "Request[%v] getWatermark Error", pkg.GetUniqLogId())
		pkg.PackErrorBody(LogGetWm, err.Error())
	} else {
		buf, err = json.Marshal(fInfo)
		pkg.PackOkWithBody(buf)
	}

	return
}

func (s *DataNode) getAllWatermark(pkg *Packet) {
	var buf []byte
	var (
		fInfoList []*storage.FileInfo
		err       error
	)
	switch pkg.StoreMode {
	case proto.TinyStoreMode:
		fInfoList, err = pkg.dataPartition.store.(*storage.TinyStore).GetAllWatermark()
	case proto.ExtentStoreMode:
		fInfoList, err = pkg.dataPartition.store.(*storage.ExtentStore).GetAllWatermark()
	}
	if err != nil {
		err = errors.Annotatef(err, "Request[%v] getAllWatermark Error", pkg.GetUniqLogId())
		pkg.PackErrorBody(LogGetAllWm, err.Error())
	} else {
		buf, err = json.Marshal(fInfoList)
		pkg.PackOkWithBody(buf)
	}
	return
}

func (s *DataNode) compactChunk(pkg *Packet) {
	cId := uint32(pkg.FileID)
	vId := pkg.PartitionID
	task := &CompactTask{
		partitionId: vId,
		chunkId:     int(cId),
		isLeader:    false,
	}
	err := s.AddCompactTask(task)
	if err != nil {
		err = errors.Annotatef(err, "Request[%v] compactChunk Error", pkg.GetUniqLogId())
		pkg.PackErrorBody(LogCompactChunk, err.Error())
		return
	}
	pkg.PackOkReply()

	return
}

func (s *DataNode) repair(pkg *Packet) {
	v := s.space.getDataPartition(pkg.PartitionID)
	if v == nil {
		err := errors.Annotatef(fmt.Errorf("dataPartition not exsit"), "Request[%v] compactChunk Error", pkg.GetUniqLogId())
		pkg.PackErrorBody(LogRepair, err.Error())
	}
	switch pkg.StoreMode {
	case proto.ExtentStoreMode:
		s.repairExtents(pkg)
	case proto.TinyStoreMode:
		s.repairTiny(pkg)
	}

	return
}
