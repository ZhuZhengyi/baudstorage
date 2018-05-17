package datanode

import (
	"encoding/json"
	"fmt"
	"github.com/tiglabs/baudstorage/proto"
	"github.com/tiglabs/baudstorage/storage"
	"github.com/tiglabs/baudstorage/util/log"
	"hash/crc32"
	"net"
)

func (s *DataNode) repairObjectRead(pkg *Packet, conn *net.TCPConn) {
	var (
		err        error
		localOid   uint64
		requireOid uint64
		chunkID    uint32
	)
	chunkID = uint32(pkg.FileID)
	requireOid = uint64(pkg.Offset + 1)
	localOid, err = pkg.vol.store.(*storage.TinyStore).GetLastOid(chunkID)
	log.LogWrite(pkg.actionMesg(ActionLeaderToFollowerOpCRepairReadPackResponse,
		fmt.Sprintf("follower require Oid[%v] localOid[%v]", requireOid, localOid), pkg.StartT, err))
	if localOid < requireOid {
		err = fmt.Errorf(" requireOid[%v] but localOid[%v]", requireOid, localOid)
		pkg.PackErrorBody(ActionLeaderToFollowerOpCRepairReadPackResponse, err.Error())
		return
	}
	err = syncData(chunkID, requireOid, localOid, pkg, conn)
	if err != nil {
		pkg.PackErrorBody(ActionLeaderToFollowerOpCRepairReadPackResponse, err.Error())
	}

	return
}

func getObjects(v *Vol, chunkID uint32, startOid, lastOid uint64) (objects []*storage.Object) {
	objects = make([]*storage.Object, 0)
	for startOid <= lastOid {
		needle, err := v.store.(*storage.TinyStore).GetObject(chunkID, uint64(startOid))
		if err != nil {
			needle = &storage.Object{Oid: uint64(startOid), Size: storage.TombstoneFileSize}
		}
		objects = append(objects, needle)
		startOid++
	}

	return
}

func postRepairData(pkg *Packet, lastOid uint64, data []byte, size int, conn *net.TCPConn) (err error) {
	pkg.Offset = int64(lastOid)
	pkg.ResultCode = proto.OpOk
	pkg.Size = uint32(size)
	pkg.Data = data
	pkg.Crc = crc32.ChecksumIEEE(pkg.Data)
	err = pkg.WriteToNoDeadLineConn(conn)
	log.LogWrite(pkg.actionMesg(ActionLeaderToFollowerOpRepairReadSendPackBuffer, conn.RemoteAddr().String(), pkg.StartT, err))

	return
}

func packObjectToBuf(databuf []byte, o *storage.Object, chunkID uint32, v *Vol) (err error) {
	o.Marshal(databuf)
	if o.Size == storage.TombstoneFileSize && o.Oid != 0 {
		return
	}
	_, err = v.store.(*storage.TinyStore).Read(chunkID, int64(o.Oid), int64(o.Size), databuf[storage.ObjectHeaderSize:])
	return
}

func syncData(chunkID uint32, startOid, endOid uint64, pkg *Packet, conn *net.TCPConn) error {
	var (
		err     error
		objects []*storage.Object
	)
	vol := pkg.vol
	objects = getObjects(vol, chunkID, startOid, endOid)
	log.LogWrite(pkg.actionMesg(ActionLeaderToFollowerOpRepairReadPackBuffer, string(len(objects)), pkg.StartT, err))
	databuf := make([]byte, PkgRepairCReadRespMaxSize)
	pos := 0
	for i := 0; i < len(objects); i++ {
		var realSize uint32
		realSize = 0
		if objects[i].Size != storage.TombstoneFileSize {
			realSize = objects[i].Size
		}
		if pos+int(realSize)+storage.ObjectHeaderSize >= PkgRepairCReadRespLimitSize {
			if err = postRepairData(pkg, objects[i-1].Oid, databuf, pos, conn); err != nil {
				return err
			}
			databuf = make([]byte, PkgRepairCReadRespMaxSize)
			pos = 0
		}
		if packObjectToBuf(databuf[pos:], objects[i], chunkID, vol); err != nil {
			return err
		}
		pos += storage.ObjectHeaderSize
		pos += int(realSize)
	}
	return postRepairData(pkg, objects[len(objects)-1].Oid, databuf, pos, conn)
}

type RepairTask struct {
	RemoteAddr     string
	RemoteFileId   uint64
	RemoteOffset   uint64
	RemoteFileSize uint64
	VolId          uint32
}

func (rt *RepairTask) toString() (m string) {
	return fmt.Sprintf("RepairTask vol[%v] remote[%v] file[%v] offset[%v] size[%v]",
		rt.VolId, rt.RemoteAddr, rt.RemoteFileId, rt.RemoteOffset, rt.RemoteFileSize)
}

func (s *DataNode) repairChunk(pkg *Packet) {
	task := new(RepairTask)
	err := json.Unmarshal(pkg.Data[:pkg.Size], task)
	if err != nil {
		pkg.PackErrorBody(LogRepair, err.Error())
		return
	}
	log.LogWrite(pkg.actionMesg(ActionFollowerToLeaderOpCRepairReadSendRequest, task.RemoteAddr, pkg.StartT, err))

	//doRepairChunk(task, pkg.ReqID)
	pkg.PackOkReply()

	return
}

//
//func doRepairChunk(task *RepairTask, notifyReqId int64) error {
//	start := time.Now().UnixNano()
//	var (
//		err error
//	)
//	pkg := NewCRepairePacket(chunkId, vol.volID, notifyReqId)
//	defer repairChunkLog(pkg, dstAddr, err)
//	chunkID := pkg.FileID
//	startObjId, _ := vol.Store.GetLastOid(chunkID)
//	pkg.Offset = int64(startObjId)
//	pkg.Opcode = OpCRepairRead
//	pkg.orgOpcode = pkg.Opcode
//	pkg.FileID = chunkID
//	conn, err := dialWithRetry(-1, dstAddr)
//	if err != nil {
//		return err
//	}
//	defer conn.Close()
//	glog.LogWrite(fmt.Sprintf(pkg.actionMesg(ActionFollowerToLeaderOpCRepairReadSendRequest, dstAddr, pkg.startT, err)))
//	if err = pkg.WriteToConn(conn, FreeBodySpace); err != nil {
//		return err
//	}
//	for {
//		localOid, _ := vol.Store.GetLastOid(chunkID)
//		if localOid >= remoteLastOid {
//			break
//		}
//		if err = pkg.ReadFromConn(conn, NoReadDeadlineTime, PkgRepairCReadRespMaxSize); err != nil {
//			return err
//		}
//		glog.LogWrite(pkg.actionMesg(ActionLeaderToFollowerOpCRepairReadRecvResponse, dstAddr, pkg.startT, err))
//		if pkg.IsErrPack() {
//			err = fmt.Errorf(" remote [%v] do failed [%v]", dstAddr, string(pkg.Data[:pkg.Size]))
//			return err
//		}
//		newlastOid := uint64(pkg.Offset)
//		if newlastOid > remoteLastOid {
//			err = fmt.Errorf("invalid offset of OpCRepairReadResp:"+
//				" %v, expect max objid is %v", pkg.Offset, remoteLastOid)
//			return err
//		}
//		glog.LogWrite(fmt.Sprintf("vol [%v] chunk[%v] doRepairChunk start fix oid [%v]-[%v]",
//			vol.volID, chunkId, startObjId, newlastOid))
//		if err = applyRepairData(startObjId, newlastOid, chunkID, pkg, vol); err != nil {
//			glog.LogWrite(err.Error())
//			return err
//		}
//		if newlastOid >= remoteLastOid {
//			break
//		}
//		pkg.Data = nil
//		glog.LogWrite(pkg.actionMesg(ActionDoRepairChunk, dstAddr, start, err))
//		startObjId = newlastOid
//	}
//
//	return err
//
//}