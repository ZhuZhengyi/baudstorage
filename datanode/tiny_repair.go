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
	"net"
)

func (dp *DataPartition) tinyRepair() {
	allMembers, err := dp.getAllMemberFileMetas()
	if err != nil {
		log.LogError(errors.ErrorStack(err))
		return
	}
	dp.generatorTinyRepairTasks(allMembers)
	err = dp.NotifyRepair(allMembers)
	if err != nil {
		log.LogError(errors.ErrorStack(err))
	}
}

func (dp *DataPartition) generatorTinyRepairTasks(allMembers []*MembersFileMetas) {
	dp.generatorFixFileSizeTasks(allMembers)
	dp.generatorTinyDeleteTasks(allMembers)

}

func (dp *DataPartition) generatorTinyDeleteTasks(allMembers []*MembersFileMetas) {
	store := dp.store.(*storage.TinyStore)
	for _, chunkInfo := range allMembers[0].objects {
		deletes := store.GetDelObjects(uint32(chunkInfo.FileIdId))
		deleteBuf := make([]byte, len(deletes)*ObjectIDSize)
		for index, deleteObject := range deletes {
			binary.BigEndian.PutUint64(deleteBuf[index*ObjectIDSize:(index+1)*ObjectIDSize], deleteObject)
		}
		for index := 0; index < len(allMembers); index++ {
			allMembers[index].NeedDeleteObjectsTasks[chunkInfo.FileIdId] = make([]byte, len(deleteBuf))
			copy(allMembers[index].NeedDeleteObjectsTasks[chunkInfo.FileIdId], deleteBuf)
		}
	}

}

func (s *DataNode) repairObjectRead(pkg *Packet, conn *net.TCPConn) {
	var (
		err        error
		localOid   uint64
		requireOid uint64
		chunkID    uint32
	)
	chunkID = uint32(pkg.FileID)
	requireOid = uint64(pkg.Offset + 1)
	localOid, err = pkg.dataPartition.store.(*storage.TinyStore).GetLastOid(chunkID)
	log.LogWrite(pkg.ActionMsg(ActionLeaderToFollowerOpCRepairReadPackResponse,
		fmt.Sprintf("follower require Oid[%v] localOid[%v]", requireOid, localOid), pkg.StartT, err))
	if localOid < requireOid {
		err = fmt.Errorf(" requireOid[%v] but localOid[%v]", requireOid, localOid)
		err = errors.Annotatef(err, "Request[%v] repairObjectRead Error", pkg.GetUniqLogId())
		pkg.PackErrorBody(ActionLeaderToFollowerOpCRepairReadPackResponse, err.Error())
		return
	}
	err = syncData(chunkID, requireOid, localOid, pkg, conn)
	if err != nil {
		err = errors.Annotatef(err, "Request[%v] SYNCDATA Error", pkg.GetUniqLogId())
		pkg.PackErrorBody(ActionLeaderToFollowerOpCRepairReadPackResponse, err.Error())
	}

	return
}

func getObjects(dp *DataPartition, chunkID uint32, startOid, lastOid uint64) (objects []*storage.Object) {
	objects = make([]*storage.Object, 0)
	for startOid <= lastOid {
		needle, err := dp.store.(*storage.TinyStore).GetObject(chunkID, uint64(startOid))
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
	log.LogWrite(pkg.ActionMsg(ActionLeaderToFollowerOpRepairReadSendPackBuffer, conn.RemoteAddr().String(), pkg.StartT, err))

	return
}

func packObjectToBuf(databuf []byte, o *storage.Object, chunkID uint32, dp *DataPartition) (err error) {
	o.Marshal(databuf)
	if o.Size == storage.TombstoneFileSize && o.Oid != 0 {
		return
	}
	_, err = dp.store.(*storage.TinyStore).Read(chunkID, int64(o.Oid), int64(o.Size), databuf[storage.ObjectHeaderSize:])
	return
}

const (
	PkgRepairCReadRespMaxSize   = 10 * util.MB
	PkgRepairCReadRespLimitSize = 15 * util.MB
)

func syncData(chunkID uint32, startOid, endOid uint64, pkg *Packet, conn *net.TCPConn) error {
	var (
		err     error
		objects []*storage.Object
	)
	dataPartition := pkg.dataPartition
	objects = getObjects(dataPartition, chunkID, startOid, endOid)
	log.LogWrite(pkg.ActionMsg(ActionLeaderToFollowerOpRepairReadPackBuffer, string(len(objects)), pkg.StartT, err))
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
		if packObjectToBuf(databuf[pos:], objects[i], chunkID, dataPartition); err != nil {
			return err
		}
		pos += storage.ObjectHeaderSize
		pos += int(realSize)
	}
	return postRepairData(pkg, objects[len(objects)-1].Oid, databuf, pos, conn)
}

func (s *DataNode) repairTiny(pkg *Packet) {

}

type RepairChunkTask struct {
	ChunkId  int
	StartObj uint64
	EndObj   uint64
}

func (dp *DataPartition) applyRepairObjects(chunkId int, data []byte, endObjectId uint64) (err error) {
	offset := 0
	store := dp.store.(*storage.TinyStore)
	var applyObjectId uint64
	dataLen := len(data)
	for {
		if offset+storage.ObjectHeaderSize > len(data) {
			break
		}
		if applyObjectId >= endObjectId {
			break
		}
		o := &storage.Object{}
		o.Unmarshal(data[offset : offset+storage.ObjectHeaderSize])
		offset += storage.ObjectHeaderSize
		if o.Size == storage.TombstoneFileSize {
			err = store.WriteDeleteDentry(o.Oid, chunkId, o.Crc)
		}
		if err != nil {
			return errors.Annotatef(err, "dataPartition[%v] chunkId[%v] oid[%v] writeDeleteDentry failed", dp.partitionId, chunkId, o.Oid)
		}
		if offset+int(o.Size) > dataLen {
			return errors.Annotatef(err, "dataPartition[%v] chunkId[%v] oid[%v] no body"+
				" expect[%v] actual[%v] failed", dp.partitionId, chunkId, o.Oid, o.Size, dataLen-(offset))
		}
		ndata := data[offset : offset+int(o.Size)]
		offset += int(o.Size)
		ncrc := crc32.ChecksumIEEE(ndata)
		if ncrc != o.Crc {
			return errors.Annotatef(err, "dataPartition[%v] chunkId[%v] oid[%v] "+
				"repair data crc  failed,expectCrc[%v] actualCrc[%v]", dp.partitionId, chunkId, o.Oid, o.Crc, ncrc)
		}
		err = store.Write(uint32(chunkId), int64(o.Oid), int64(o.Size), ndata, o.Crc)
		if err != nil {
			return errors.Annotatef(err, "dataPartition[%v] chunkId[%v] oid[%v] write failed", dp.partitionId, chunkId, o.Oid)
		}
		applyObjectId = o.Oid
	}
	return nil
}

func (s *DataNode) streamRepairObjects(remoteFileInfo *storage.FileInfo, dp *DataPartition) (err error) {
	store := dp.store.(*storage.TinyStore)
	localChunkInfo, err := store.GetWatermark(uint64(remoteFileInfo.FileIdId))
	if err != nil {
		return errors.Annotatef(err, "streamRepairObjects GetWatermark error")
	}
	task := &RepairChunkTask{ChunkId: remoteFileInfo.FileIdId, StartObj: localChunkInfo.Size + 1, EndObj: remoteFileInfo.Size}
	request := NewStreamChunkRepairReadPacket(dp.partitionId, remoteFileInfo.FileIdId)
	request.Data, _ = json.Marshal(task)
	var conn *net.TCPConn
	conn, err = gConnPool.Get(remoteFileInfo.Source)
	if err != nil {
		return errors.Annotatef(err, "streamRepairObjects get conn from host[%v] error", remoteFileInfo.Source)
	}
	err = request.WriteToConn(conn)
	if err != nil {
		gConnPool.Put(conn,ForceCloseConnect)
		return errors.Annotatef(err, "streamRepairObjects send streamRead to host[%v] error", remoteFileInfo.Source)
	}
	for {
		localExtentInfo, err := store.GetWatermark(uint64(remoteFileInfo.FileIdId))
		if err != nil {
			conn.Close()
			return errors.Annotatef(err, "streamRepairObjects GetWatermark error")
		}
		if localExtentInfo.Size >= remoteFileInfo.Size {
			gConnPool.Put(conn,ForceCloseConnect)
			break
		}
		err = request.ReadFromConn(conn, proto.ReadDeadlineTime)
		if err != nil {
			gConnPool.Put(conn,ForceCloseConnect)
			return errors.Annotatef(err, "streamRepairObjects recive data error")
		}
		newlastOid := uint64(request.Offset)
		if newlastOid > uint64(remoteFileInfo.FileIdId) {
			gConnPool.Put(conn,ForceCloseConnect)
			err = fmt.Errorf("invalid offset of OpCRepairReadResp:"+
				" %v, expect max objid is %v", newlastOid, remoteFileInfo.FileIdId)
			return err
		}
		err = dp.applyRepairObjects(remoteFileInfo.FileIdId, request.Data, newlastOid)
		if err != nil {
			gConnPool.Put(conn,ForceCloseConnect)
			err = errors.Annotatef(err, "streamRepairObjects apply data failed")
			return err
		}
	}
	return
}
