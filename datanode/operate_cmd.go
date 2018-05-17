package datanode

import (
	"errors"
	"fmt"
	"strconv"
)

var (
	ErrorUnknowOp = errors.New("UnknowOpcodeErr")
)

func (s *DataNode) processCmd(cmd *Cmd) (resp *CmdResp) {
	glog.LogInfo("Process cmd from master:", cmd.Code, cmd.GetIDInfo())
	switch cmd.Code {
	case OpCreateVol:
		resp = s.createVol(cmd)
	case OpDeleteVol:
		resp = s.deleteVol(cmd)
	case OpDeleteFile:
		resp = s.deleteFile(cmd)
	case OpReplicationFile:
		if s.storeType == ChunkStoreType {
			resp = cmd.PackOkReply()
			break
		}
		resp = s.repaireExtent(cmd)
	case OpVolSnapshot:
		resp = s.volSnapshot(cmd)
	case OpCompactOn:
		s.compactWorking = true
		resp = &CmdResp{Result: CmdSuccess, ID: cmd.ID}
	case OpCompactOff:
		s.compactWorking = false
		resp = &CmdResp{Result: CmdSuccess, ID: cmd.ID}
	default:
		resp = cmd.PackErrorReply(ErrorUnknowOp.Error(), ErrorUnknowOp.Error()+strconv.Itoa(int(cmd.Code)))
	}
	resp.Addr = s.ip + ":" + s.port
	return
}

func (s *DataNode) chooseSuitDisk(size int) (has bool, diskPath string) {
	var maxAvail int64

	for _, d := range s.smMgr.disks {
		avail := d.GetCreateVolsAvailSpace()
		if avail < int64(size) {
			d.SetStatus(ReadOnlyDisk)
			continue
		}
		if maxAvail < avail && d.GetRealAvailSpace() >= int64(size) && d.GetStatus() == ReadWriteDisk {
			maxAvail = avail
			diskPath = d.Path
			glog.LogInfo("chooseSuitDisk, tmpAvail:", maxAvail/GB, "G Path:", diskPath)
		}
	}
	if maxAvail >= int64(size) {
		has = true
		glog.LogInfo("chooseSuitDisk, maxAvail:", maxAvail/GB, "G Path:", diskPath)
	} else {
		glog.LogWarn("chooseSuitDisk, maxAvail:", maxAvail/GB, "G Path:", diskPath)
	}

	return
}

func (s *DataNode) createVol(cmd *Cmd) (resp *CmdResp) {
	var createOk bool
	if _, ok := s.smMgr.IsExistVol(cmd.VolID); ok {
		resp = cmd.PackOkReply()
		return
	}
	for i := 0; i < ChooseDiskTimes; i++ {
		has, path := s.chooseSuitDisk(cmd.VolSize)
		if !has {
			continue
		}
		vol, err := NewVolAgent(cmd.VolID, cmd.VolSize, path, JoinVolName(cmd.VolID, cmd.VolSize),
			s.clusterId, s.ip+":"+s.port, s.chunkCheckerInterval, s.storeType, s, NewStoreMode)
		if err != nil {
			glog.LogError(LogCreateVol+":"+cmd.GetIDInfo()+" err:", err)
			continue
		}
		s.smMgr.AddVol(path, vol)
		go vol.StartCheck()
		createOk = true
		break
	}

	if !createOk {
		resp = cmd.PackErrorReply(CreateVolErr, fmt.Sprintf("no space for createvol"))
		return
	}
	resp = cmd.PackOkReply()
	return
}

func (s *DataNode) deleteVol(cmd *Cmd) (resp *CmdResp) {
	vol, ok := s.smMgr.IsExistVol(cmd.VolID)
	if ok {
		vol.deleted = true
		s.smMgr.DelVol(vol)
	}

	resp = cmd.PackOkReply()
	return
}

func (s *DataNode) deleteFile(cmd *Cmd) (resp *CmdResp) {
	vol, ok := s.smMgr.IsExistVol(cmd.VolID)
	if !ok {
		resp = cmd.PackErrorReply(ErrVolIsMissing.Error(), fmt.Sprintf("vol not exsit"))
		return
	}

	eID, err := strconv.Atoi(cmd.VolFileID)
	if err != nil {
		resp = cmd.PackErrorReply(err.Error(), err.Error())
		return
	}
	fsize, _ := vol.Store.GetWatermark(uint32(eID))
	fsize = fsize + ExtentHeaderSize
	if err := vol.Store.Delete(uint32(eID)); err != nil {
		resp = cmd.PackErrorReply(LogDelFile, err.Error())
		return
	}
	resp = cmd.PackOkReply()

	return
}

func (s *DataNode) loadVol(cmd *Cmd) (resp *CmdResp) {
	vol, ok := s.smMgr.IsExistVol(cmd.VolID)
	if !ok {
		resp = cmd.PackErrorReply(LogVolSnapshot, ErrVolIsMissing.Error())
		return
	}

	filesInfo, err := vol.Store.Snapshot()
	if err != nil {
		resp = cmd.PackErrorReply(LogVolSnapshot, err.Error())
		return
	}

	resp = cmd.PackOkReply()
	resp.VolSnapshot = filesInfo
	return
}

func (s *DataNode) repaireExtent(cmd *Cmd) (resp *CmdResp) {
	vol, ok := s.smMgr.IsExistVol(cmd.VolID)
	if !ok {
		resp = cmd.PackErrorReply(ErrVolIsMissing.Error(), fmt.Sprintf("vol not exsit"))
		return
	}

	fID, err := strconv.Atoi(cmd.VolFileID)
	if err != nil {
		resp = cmd.PackErrorReply(LogRepair, err.Error())
		return
	}
	if vol.Store.FileExist(uint32(fID)) {
		resp = cmd.PackOkReply()
	}

	if err := vol.Store.Create(uint32(fID)); err != nil {
		resp = cmd.PackErrorReply(LogRepair, err.Error())
		return
	}

	wmPkg := NewGetWatermarkPacket(uint32(fID), GetReqID(), vol.volID)
	wmPkg.StoreType = ExtentStoreType
	wmPkg.ReqID = GetReqID()
	err = sendAndRecvPkg(wmPkg, cmd.TargetAddr)
	if err != nil {
		resp = cmd.PackErrorReply(LogRepair, err.Error())
		return
	}

	pkg := NewERepairePacket(uint32(fID), GetReqID(), vol.volID)
	pkg.StoreType = ExtentStoreType
	_, err = doRepairExtent(0, wmPkg.Offset, PkgRepairDataMaxSize, pkg, vol, cmd.TargetAddr)
	if err != nil {
		resp = cmd.PackErrorReply(LogRepair, err.Error())
	} else {
		resp = cmd.PackOkReply()
	}

	return
}
