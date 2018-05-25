package master

import (
	"fmt"
	"github.com/tiglabs/baudstorage/proto"
	"github.com/tiglabs/baudstorage/util/log"
	"math/rand"
)

/*this struct define chunk file metadata on  dataNode */
type FileMetaOnNode struct {
	Crc       uint32
	LocAddr   string
	LocIndex  uint8
	LastObjID uint64
	NeedleCnt int
}

type FileInCore struct {
	Name       string
	MarkDel    bool
	LastModify int64
	Metas      []*FileMetaOnNode
}

func NewFileMetaOnNode(volCrc uint32, volLoc string, volLocIndex int, lastObjID uint64, needleCnt int) (fm *FileMetaOnNode) {
	fm = new(FileMetaOnNode)
	fm.Crc = volCrc
	fm.LocAddr = volLoc
	fm.LocIndex = uint8(volLocIndex)
	fm.LastObjID = lastObjID
	fm.NeedleCnt = needleCnt
	return
}

func (fm *FileMetaOnNode) getLocationAddr() (loc string) {
	return fm.LocAddr
}

func (fm *FileMetaOnNode) getFileCrc() (crc uint32) {
	return fm.Crc
}

func NewFileInCore(name string) (fc *FileInCore) {
	fc = new(FileInCore)
	fc.Name = name
	fc.MarkDel = false
	fc.Metas = make([]*FileMetaOnNode, 0)

	return
}

/*use a File and volLocation update FileInCore,
range all FileInCore.NodeInfos,update crc and reportTime*/
func (fc *FileInCore) updateFileInCore(volID uint64, vf *proto.File, volLoc *Vol, volLocIndex int) {
	if vf.MarkDel == true {
		fc.MarkDel = true
	}

	if vf.Modified > fc.LastModify {
		fc.LastModify = vf.Modified
	}

	isFind := false
	for i := 0; i < len(fc.Metas); i++ {
		if fc.Metas[i].getLocationAddr() == volLoc.addr {
			fc.Metas[i].Crc = vf.Crc
			isFind = true
			break
		}
	}

	if isFind == false {
		fm := NewFileMetaOnNode(vf.Crc, volLoc.addr, volLocIndex, vf.LastObjID, vf.NeedleCnt)
		fc.Metas = append(fc.Metas, fm)
	}

}

func (fc *FileInCore) generatorDeleteFileTask(volID uint64) (tasks []*proto.AdminTask) {
	tasks = make([]*proto.AdminTask, 0)
	for i := 0; i < len(fc.Metas); i++ {
		fm := fc.Metas[i]
		vfDeleteNode := fm.getLocationAddr()
		msg := fmt.Sprintf(DeleteFileInCoreInfo+"volID:%v  File:%v  Delete on Node:%v ", volID, fc.Name, vfDeleteNode)
		log.LogDebug(msg)
		tasks = append(tasks, proto.NewAdminTask(proto.OpDeleteFile, vfDeleteNode, newDeleteFileRequest(volID, fc.Name)))
	}

	return
}

/*delete File nodeInfo on location*/
func (fc *FileInCore) deleteFileInNode(volID uint64, loc *Vol) {
	for i := 0; i < len(fc.Metas); i++ {
		fm := fc.Metas[i]
		if fm.LocAddr == loc.addr {
			afterNodes := fc.Metas[i+1:]
			fc.Metas = fc.Metas[0:i]
			fc.Metas = append(fc.Metas, afterNodes...)
			break
		}
	}
}

/*get volFile replication source exclude badLoc*/
func (fc *FileInCore) getLiveLocExcludeBadLoc(volLocs []*Vol, badLoc *Vol) (loc *Vol, err error) {
	err = VolPersistedNotAnyReplicates

	if len(volLocs) < 0 {
		return
	}
	if loc, err = fc.randSelectReplicateSource(volLocs, badLoc); err != nil {
		return fc.orderSelectReplicateSource(volLocs, badLoc)
	}
	return
}

func (fc *FileInCore) randSelectReplicateSource(volLocs []*Vol, badLoc *Vol) (loc *Vol, err error) {
	index := rand.Intn(len(volLocs))
	loc = volLocs[index]
	if loc.addr != badLoc.addr && fc.locIsInNodeInfos(loc) == true {
		return
	}

	return nil, VolPersistedNotAnyReplicates
}

func (fc *FileInCore) orderSelectReplicateSource(volLocs []*Vol, badLoc *Vol) (loc *Vol, err error) {
	for i := 0; i < len(volLocs); i++ {
		loc = volLocs[i]
		if loc.addr != badLoc.addr && fc.locIsInNodeInfos(loc) == true {
			return
		}
	}

	return nil, VolPersistedNotAnyReplicates
}

func (fc *FileInCore) locIsInNodeInfos(loc *Vol) (ok bool) {
	for i := 0; i < len(fc.Metas); i++ {
		if fc.Metas[i].LocAddr == loc.addr {
			return true
		}
	}

	return
}

func (fc *FileInCore) getFileMetaByVolAddr(vol *Vol) (fm *FileMetaOnNode, ok bool) {
	for i := 0; i < len(fc.Metas); i++ {
		fm = fc.Metas[i]
		if fm.LocAddr == vol.addr {
			ok = true
			return
		}
	}

	return
}
