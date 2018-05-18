package datanode

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"github.com/juju/errors"
	"github.com/tiglabs/baudstorage/proto"
	"github.com/tiglabs/baudstorage/storage"
	"github.com/tiglabs/baudstorage/util"
	"github.com/tiglabs/baudstorage/util/log"
	"hash/crc32"
	"io"
	"net"
	"strconv"
	"strings"
	"time"
)

func (s *DataNode) operatePacket(pkg *Packet, c *net.TCPConn) {
	orgSize := pkg.Size
	start := time.Now().UnixNano()
	var err error
	defer func() {
		resultSize := pkg.Size
		pkg.Size = orgSize
		if pkg.IsErrPack() {
			err = fmt.Errorf("operation[%v] error[%v]", proto.GetOpMesg(pkg.Opcode), string(pkg.Data[:resultSize]))
		} else {
			if pkg.IsReadReq() {
				log.LogRead(pkg.actionMesg(proto.GetOpMesg(pkg.Opcode), LocalProcessAddr, start, nil))
			} else {
				log.LogWrite(pkg.actionMesg(proto.GetOpMesg(pkg.Opcode), LocalProcessAddr, start, nil))
			}
		}
		pkg.Size = resultSize
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
	case proto.OpCreateVol:
		s.createVol(pkg)
	case proto.OpLoadVol:
		s.loadVol(pkg)
	case proto.OpDeleteVol:
		s.deleteVol(pkg)
	default:
		pkg.PackErrorBody(ErrorUnknowOp.Error(), ErrorUnknowOp.Error()+strconv.Itoa(int(pkg.Opcode)))
	}

	return
}

func (s *DataNode) createFile(pkg *Packet) {
	var err error
	switch pkg.StoreMode {
	case proto.TinyStoreMode:
		err = errors.Annotatef(ErrStoreTypeUnmatch, " CreateFile only support ExtentMode Vol")
	case proto.ExtentStoreMode:
		err = pkg.vol.store.(*storage.ExtentStore).MarkDelete(pkg.FileID, pkg.Offset, int64(pkg.Size))
	}
	if err != nil {
		pkg.PackErrorBody(LogCreateFile, err.Error())
	} else {
		pkg.PackOkReply()
	}

	return
}

func (s *DataNode) createVol(pkg *Packet) {
	task := &proto.AdminTask{}
	json.Unmarshal(pkg.Data, task)
	if task.OpCode == proto.OpCreateVol {
		request := task.Request.(*proto.CreateVolRequest)
		s.space.chooseDiskAndCreateVol(request.VolId, request.VolType, request.VolSize)
	} else {
		data, _ := json.Marshal(task)
		s.PostToMaster(data, "/node/report")
	}
}

func (s *DataNode) markDel(pkg *Packet) {
	var err error
	switch pkg.StoreMode {
	case proto.TinyStoreMode:
		err = pkg.vol.store.(*storage.TinyStore).MarkDelete(uint32(pkg.FileID), pkg.Offset, int64(pkg.Size))
	case proto.ExtentStoreMode:
		err = pkg.vol.store.(*storage.ExtentStore).MarkDelete(pkg.FileID, pkg.Offset, int64(pkg.Size))
	}
	if err != nil {
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
		err = pkg.vol.store.(*storage.TinyStore).Write(uint32(pkg.FileID), pkg.Offset, int64(pkg.Size), pkg.Data, pkg.Crc)
		s.AddDiskErrs(pkg.VolID, err, WriteFlag)
	case proto.ExtentStoreMode:
		err = errors.Annotatef(ErrStoreTypeUnmatch, " Append only support TinyVol")
	}
	if err != nil {
		pkg.PackErrorBody(LogMarkDel, err.Error())
	} else {
		pkg.PackOkReply()
	}

	return
}

func (s *DataNode) read(pkg *Packet) {
	pkg.Data = make([]byte, pkg.Size)
	var err error
	pkg.Crc, err = pkg.vol.store.(*storage.TinyStore).Read(uint32(pkg.FileID), pkg.Offset, int64(pkg.Size), pkg.Data)
	if err != nil {
		err = errors.Annotatef(err, "Request[%v] Read Error", pkg.GetUniqLogId())
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
		pkg.PackErrorBody(LogRepairNeedles, "Invalid args for OpSyncNeedle")
		return
	}
	needles := make([]uint64, 0)
	for i := 0; i < int(pkg.Size/storage.ObjectIdLen); i++ {
		needle := binary.BigEndian.Uint64(pkg.Data[i*storage.ObjectIdLen : (i+1)*storage.ObjectIdLen])
		needles = append(needles, needle)
	}
	if err := pkg.vol.store.(*storage.TinyStore).ApplyDelObjects(uint32(pkg.FileID), needles); err != nil {
		pkg.PackErrorBody(LogRepair, "err OpSyncNeedle: "+err.Error())
		return
	}
	pkg.PackOkReply()
	return
}

func (s *DataNode) streamRead(pkg *Packet, c net.Conn) {
	var (
		err error
	)
	needReplySize := pkg.Size
	offset := pkg.Offset
	for {
		if needReplySize <= 0 {
			break
		}
		err = nil
		currReadSize := uint32(util.Min(int(needReplySize), storage.BlockSize))
		pkg.Data = make([]byte, currReadSize)
		pkg.Crc, err = pkg.vol.store.(*storage.ExtentStore).Read(pkg.FileID, offset, int64(currReadSize), pkg.Data)
		if err != nil {
			pkg.PackErrorBody(ActionStreamRead, err.Error())
			s.AddDiskErrs(pkg.VolID, err, ReadFlag)
			pkg.WriteToConn(c)
			return
		}
		pkg.Size = currReadSize
		pkg.Opcode = proto.OpOk
		if err = pkg.WriteToConn(c); err != nil {
			return
		}
		needReplySize -= currReadSize
		offset += int64(currReadSize)
	}
	return
}

func (s *DataNode) getWatermark(pkg *Packet) {
	var buf []byte
	var (
		finfo *storage.FileInfo
		err   error
	)
	switch pkg.StoreMode {
	case proto.TinyStoreMode:
		finfo, err = pkg.vol.store.(*storage.TinyStore).GetWatermark(uint32(pkg.FileID))
	case proto.ExtentStoreMode:
		finfo, err = pkg.vol.store.(*storage.ExtentStore).GetWatermark(pkg.FileID)
	}
	if err != nil {
		pkg.PackErrorBody(LogGetWm, err.Error())
	} else {
		buf, err = json.Marshal(finfo)
		pkg.PackOkWithBody(buf)
	}

	return
}

func (s *DataNode) getAllWatermark(pkg *Packet) {
	var buf []byte
	var (
		finfos []*storage.FileInfo
		err    error
	)
	switch pkg.StoreMode {
	case proto.TinyStoreMode:
		finfos, err = pkg.vol.store.(*storage.TinyStore).GetAllWatermark()
	case proto.ExtentStoreMode:
		finfos, err = pkg.vol.store.(*storage.ExtentStore).GetAllWatermark()
	}
	if err != nil {
		pkg.PackErrorBody(LogGetAllWm, err.Error())
	} else {
		buf, err = json.Marshal(finfos)
		pkg.PackOkWithBody(buf)
	}
	return
}

func (s *DataNode) compactChunk(pkg *Packet) {
	cId := uint32(pkg.FileID)
	vId := pkg.VolID
	task := &CompactTask{
		volId:    vId,
		chunkId:  int(cId),
		isLeader: false,
	}
	err := s.AddCompactTask(task)
	if err != nil {
		pkg.PackErrorBody(LogCompactChunk, err.Error())
		return
	}
	pkg.PackOkReply()

	return
}

func (s *DataNode) repair(pkg *Packet) {
	v := s.space.getVol(pkg.VolID)
	if v == nil {
		pkg.PackErrorBody(LogRepair, fmt.Sprintf("vol[%v] not exsits", pkg.VolID))
	}
	switch pkg.StoreMode {
	case proto.ExtentStoreMode:
		s.repairExtents(pkg)
	case proto.TinyStoreMode:
		s.repairTiny(pkg)
	}

	return
}
