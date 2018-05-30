package datanode

import (
	"encoding/json"
	"github.com/juju/errors"
	"github.com/tiglabs/baudstorage/proto"
	"github.com/tiglabs/baudstorage/storage"
	"github.com/tiglabs/baudstorage/util/log"
	"net"
	"time"
)

func (v *Vol) checkExtent() {
	ticker := time.Tick(time.Second * 100)
	for {
		select {
		case <-ticker:
			if err := v.parseVolMember(); err != nil {
				continue
			}
			v.extentsRepair()
		case <-v.exitCh:
			return
		}
	}
}

type MembersFileMetas struct {
	Index                  int
	extents                map[int]*storage.FileInfo
	NeedDeleteExtentsTasks []*storage.FileInfo
	NeedAddExtentsTasks    []*storage.FileInfo
	NeedFixFileSizeTasks   []*storage.FileInfo

	objects                map[int]*storage.FileInfo
	NeedDeleteObjectsTasks map[int][]byte
}

func NewMembersFiles() (mf *MembersFileMetas) {
	mf = new(MembersFileMetas)
	mf.extents = make(map[int]*storage.FileInfo)
	mf.NeedDeleteExtentsTasks = make([]*storage.FileInfo, 0)
	mf.NeedAddExtentsTasks = make([]*storage.FileInfo, 0)
	mf.NeedDeleteObjectsTasks = make(map[int][]byte)
	mf.objects = make(map[int]*storage.FileInfo)

	return
}

func (v *Vol) extentsRepair() {
	allMembers, err := v.getAllMemberFileMetas()
	if err != nil {
		log.LogError(errors.ErrorStack(err))
		return
	}
	v.generatorExtentsRepairTasks(allMembers)
	err = v.NotifyRepair(allMembers)
	if err != nil {
		log.LogError(errors.ErrorStack(err))
	}
	for _, fixExtentFile := range allMembers[0].NeedFixFileSizeTasks {
		v.server.streamRepairExtent(fixExtentFile, v)
	}
}

func (v *Vol) getAllMemberFileMetas() (allMembers []*MembersFileMetas, err error) {
	allMembers = make([]*MembersFileMetas, v.members.VolGoal)
	var files []*storage.FileInfo
	switch v.volMode {
	case proto.ExtentVol:
		store := v.store.(*storage.ExtentStore)
		files, err = store.GetAllWatermark()
	case proto.TinyVol:
		store := v.store.(*storage.TinyStore)
		files, err = store.GetAllWatermark()
	}
	mf := NewMembersFiles()
	if err != nil {
		err = errors.Annotatef(err, "getAllMemberFileMetas vol[%v] GetAllWaterMark", v.volId)
		return
	}
	for _, fi := range files {
		mf.extents[fi.FileIdId] = fi
	}
	allMembers[0] = mf
	p := NewGetAllWaterMarker(v.volId, proto.ExtentStoreMode)
	for i := 1; i < len(v.members.VolHosts); i++ {
		var conn net.Conn
		target := v.members.VolHosts[i]
		conn, err = connPool.Get(target)
		if err != nil {
			err = errors.Annotatef(err, "getAllMemberFileMetas  vol[%v] get host[%v] connect", v.volId, target)
			return
		}
		err = p.WriteToConn(conn)
		if err != nil {
			conn.Close()
			err = errors.Annotatef(err, "getAllMemberFileMetas vol[%v] write to host[%v]", v.volId, target)
			return
		}
		err = p.ReadFromConn(conn, proto.ReadDeadlineTime)
		if err != nil {
			conn.Close()
			err = errors.Annotatef(err, "getAllMemberFileMetas vol[%v] read from host[%v]", v.volId, target)
			return
		}
		mf := NewMembersFiles()
		err = json.Unmarshal(p.Data[:p.Size], mf)
		if err != nil {
			connPool.Put(conn)
			err = errors.Annotatef(err, "getAllMemberFileMetas json unmarsh [%v]", v.volId, string(p.Data[:p.Size]))
			return
		}
		for _, fi := range files {
			mf.extents[fi.FileIdId] = fi
		}
		allMembers[i] = mf
	}
	return
}

func (v *Vol) generatorExtentsRepairTasks(allMembers []*MembersFileMetas) {
	v.generatorAddExtentsTasks(allMembers) //add extentTask
	v.generatorFixFileSizeTasks(allMembers)
	v.generatorDeleteExtentsTasks(allMembers)

}

/* pasre all extent,select maxExtentSize to member index map
 */
func (v *Vol) mapMaxSizeExtentToIndex(allMembers []*MembersFileMetas) (maxSizeExtentMap map[int]int) {
	leader := allMembers[0]
	maxSizeExtentMap = make(map[int]int)
	for fileId, _ := range leader.extents { //range leader all extentFiles
		maxSizeExtentMap[fileId] = 0
		var maxFileSize uint64
		for index := 0; index < len(allMembers); index++ {
			member := allMembers[index]
			_, ok := member.extents[fileId]
			if !ok {
				continue
			}
			if maxFileSize < member.extents[fileId].Size {
				maxFileSize = member.extents[fileId].Size
				maxSizeExtentMap[fileId] = index //map maxSize extentId to allMembers index
			}
		}
	}
	return
}

/*generator add extent if follower not have this extent*/
func (v *Vol) generatorAddExtentsTasks(allMembers []*MembersFileMetas) {
	leader := allMembers[0]
	leaderAddr := v.server.localServeAddr
	for fileId, leaderFile := range leader.extents {
		for index := 1; index < len(allMembers); index++ {
			follower := allMembers[index]
			if _, ok := follower.extents[fileId]; !ok {
				addFile := &storage.FileInfo{Source: leaderAddr, FileIdId: fileId, Size: leaderFile.Size}
				follower.NeedAddExtentsTasks = append(follower.NeedAddExtentsTasks, addFile)
			}
		}
	}
}

/*generator fix extent Size ,if all members  Not the same length*/
func (v *Vol) generatorFixFileSizeTasks(allMembers []*MembersFileMetas) {
	leader := allMembers[0]
	maxSizeExtentMap := v.mapMaxSizeExtentToIndex(allMembers) //map maxSize extentId to allMembers index
	for fileId, _ := range leader.extents {
		maxSizeExtentIdIndex := maxSizeExtentMap[fileId]
		maxSize := allMembers[maxSizeExtentIdIndex].extents[fileId].Size
		sourceAddr := v.members.VolHosts[maxSizeExtentIdIndex]
		for index := 0; index < len(allMembers); index++ {
			if index == maxSizeExtentIdIndex {
				continue
			}
			extentInfo, ok := allMembers[index].extents[fileId]
			if !ok {
				continue
			}
			if extentInfo.Size < maxSize {
				fixExtent := &storage.FileInfo{Source: sourceAddr, FileIdId: fileId, Size: maxSize}
				allMembers[index].NeedFixFileSizeTasks = append(allMembers[index].NeedFixFileSizeTasks, fixExtent)
			}
		}
	}
}

/*generator fix extent Size ,if all members  Not the same length*/
func (v *Vol) generatorDeleteExtentsTasks(allMembers []*MembersFileMetas) {
	store := v.store.(*storage.ExtentStore)
	deletes := store.GetDelObjects()
	leaderAddr := v.server.localServeAddr
	for _, deleteFileId := range deletes {
		for index := 1; index < len(allMembers); index++ {
			follower := allMembers[index]
			if _, ok := follower.extents[int(deleteFileId)]; ok {
				deleteFIle := &storage.FileInfo{Source: leaderAddr, FileIdId: int(deleteFileId), Size: 0}
				follower.NeedDeleteExtentsTasks = append(follower.NeedDeleteExtentsTasks, deleteFIle)
			}
		}
	}
}

/*notify follower to repair vol store*/
func (v *Vol) NotifyRepair(members []*MembersFileMetas) (err error) {
	storeMode := proto.ExtentStoreMode
	if v.volMode == proto.TinyVol {
		storeMode = proto.TinyStoreMode
	}
	p := NewNotifyRepair(v.volId, storeMode)
	for i := 1; i < len(members); i++ {
		var conn net.Conn
		target := v.members.VolHosts[i]
		conn, err = connPool.Get(target)
		if err != nil {
			continue
		}
		p.Data, err = json.Marshal(members[i])
		p.Size = uint32(len(p.Data))
		err = p.WriteToConn(conn)
		if err != nil {
			conn.Close()
			continue
		}
		connPool.Put(conn)
	}

	return
}

/*if follower recive OpNotifyRepair,then do it*/
func (s *DataNode) repairExtents(pkg *Packet) {
	mf := NewMembersFiles()
	json.Unmarshal(pkg.Data, mf)
	store := pkg.vol.store.(*storage.ExtentStore)
	for _, deleteExtentId := range mf.NeedDeleteExtentsTasks {
		store.MarkDelete(uint64(deleteExtentId.FileIdId), 0, 0)
	}
	for _, addExtent := range mf.NeedAddExtentsTasks {
		if store.IsExsitExtent(uint64(addExtent.FileIdId)) {
			continue
		}
		err := store.Create(uint64(addExtent.FileIdId))
		if err != nil {
			continue
		}
		fixFileSizeTask := &storage.FileInfo{Source: addExtent.Source, FileIdId: addExtent.FileIdId, Size: addExtent.Size}
		mf.NeedFixFileSizeTasks = append(mf.NeedFixFileSizeTasks, fixFileSizeTask)
	}

	for _, fixExtent := range mf.NeedFixFileSizeTasks {
		if !store.IsExsitExtent(uint64(fixExtent.FileIdId)) {
			continue
		}
		err := s.streamRepairExtent(fixExtent, pkg.vol)
		if err != nil {
			localExtentInfo, err1 := store.GetWatermark(uint64(fixExtent.FileIdId))
			if err != nil {
				err = errors.Annotatef(err1, "not exsit")
			}
			err = errors.Annotatef(err, "vol[%v] extent[%v] streamRepairExtentFailed "+
				"leaderExtentInfo[%v] localExtentInfo[%v]", fixExtent.ToString(), localExtentInfo.ToString())
			log.LogError(errors.ErrorStack(err))
		}
	}
}

func (s *DataNode) streamRepairExtent(remoteExtentInfo *storage.FileInfo, v *Vol) (err error) {
	store := v.store.(*storage.ExtentStore)
	if !store.IsExsitExtent(uint64(remoteExtentInfo.FileIdId)) {
		return nil
	}
	localExtentInfo, err := store.GetWatermark(uint64(remoteExtentInfo.FileIdId))
	if err != nil {
		return errors.Annotatef(err, "streamRepairExtent GetWatermark error")
	}
	needFixSize := remoteExtentInfo.Size - localExtentInfo.Size
	request := NewStreamReadPacket(v.volId, remoteExtentInfo.FileIdId, int(localExtentInfo.Size), int(needFixSize))
	var conn net.Conn
	conn, err = connPool.Get(remoteExtentInfo.Source)
	if err != nil {
		return errors.Annotatef(err, "streamRepairExtent get conn from host[%v] error", remoteExtentInfo.Source)
	}
	err = request.WriteToConn(conn)
	if err != nil {
		conn.Close()
		return errors.Annotatef(err, "streamRepairExtent send streamRead to host[%v] error", remoteExtentInfo.Source)
	}
	for {
		localExtentInfo, err := store.GetWatermark(uint64(remoteExtentInfo.FileIdId))
		if err != nil {
			conn.Close()
			return errors.Annotatef(err, "streamRepairExtent GetWatermark error")
		}
		if localExtentInfo.Size >= remoteExtentInfo.Size {
			connPool.Put(conn)
			break
		}
		err = request.ReadFromConn(conn, proto.ReadDeadlineTime)
		if err != nil {
			conn.Close()
			return errors.Annotatef(err, "streamRepairExtent recive data error")
		}
		err = store.Write(uint64(localExtentInfo.FileIdId), int64(localExtentInfo.Size), int64(request.Size), request.Data, request.Crc)
		if err != nil {
			conn.Close()
			return errors.Annotatef(err, "streamRepairExtent repair data error")
		}
	}
	return

}
