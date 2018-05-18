package datanode

import (
	"encoding/json"
	"fmt"
	"github.com/juju/errors"
	"github.com/laohanlinux/riot/store"
	"github.com/tiglabs/baudstorage/proto"
	"github.com/tiglabs/baudstorage/sdk/stream"
	"github.com/tiglabs/baudstorage/storage"
	"github.com/tiglabs/baudstorage/util/log"
	"go/token"
	"golang.org/x/text/cmd/gotext/examples/extract_http/pkg"
	"net"
	"time"
)

func (v *Vol) checkExtent() {
	ticker := time.Tick(time.Second * 100)
	for {
		select {
		case <-ticker:
			v.extentRepair()
		}
	}
}

type MembersFiles struct {
	Index            int
	HaveFiles        map[int]*storage.ExtentInfo
	NeedDeleteFiles  []*storage.ExtentInfo
	NeedAddFiles     []*storage.ExtentInfo
	NeedFixSizeFiles []*storage.ExtentInfo
}

func NewMembersFiles() (mf *MembersFiles) {
	mf = new(MembersFiles)
	mf.HaveFiles = make(map[int]*storage.ExtentInfo)
	mf.NeedDeleteFiles = make([]*storage.ExtentInfo, 0)
	mf.NeedAddFiles = make([]*storage.ExtentInfo, 0)
	return
}

func (v *Vol) extentRepair() {
	members, err := v.extentGetMembersExtents()
	if err != nil {
		log.LogError(errors.ErrorStack(err))
		return
	}
	v.extentFillDeleteOrAdd(members)
	err = v.extentNotifyRepair(members)
	if err != nil {
		log.LogError(errors.ErrorStack(err))
	}

}

func (v *Vol) extentGetMembersExtents() (members []*MembersFiles, err error) {
	members = make([]*MembersFiles, v.members.VolGoal)
	store := v.store.(*storage.ExtentStore)
	mf := NewMembersFiles()
	var files []*storage.ExtentInfo
	files, err = store.GetAllWatermark()
	if err != nil {
		err = errors.Annotatef(err, "extentGetMembersExtents vol[%v] GetAllWaterMark", v.volId)
		return
	}
	for _, fi := range files {
		mf.HaveFiles[fi.ExtentId] = fi
	}
	members[0] = mf
	p := NewGetAllWaterMarker(v.volId, proto.ExtentStoreMode)
	for i := 1; i < len(v.members.VolHosts); i++ {
		var conn net.Conn
		target := v.members.VolHosts[i]
		conn, err = v.server.ConnPool.Get(target)
		if err != nil {
			err = errors.Annotatef(err, "extentGetMembersExtents  vol[%v] get host[%v] connect", v.volId, target)
			return
		}
		err = p.WriteToConn(conn)
		if err != nil {
			conn.Close()
			err = errors.Annotatef(err, "extentGetMembersExtents vol[%v] write to host[%v]", v.volId, target)
			return
		}
		err = p.ReadFromConn(conn, proto.ReadDeadlineTime)
		if err != nil {
			conn.Close()
			err = errors.Annotatef(err, "extentGetMembersExtents vol[%v] read from host[%v]", v.volId, target)
			return
		}
		mf := NewMembersFiles()
		err = json.Unmarshal(p.Data[:p.Size], mf)
		if err != nil {
			v.server.ConnPool.Put(conn)
			err = errors.Annotatef(err, "extentGetMembersExtents json unmarsh [%v]", v.volId, string(p.Data[:p.Size]))
			return
		}
		for _, fi := range files {
			mf.HaveFiles[fi.ExtentId] = fi
		}
		members[i] = mf
	}
	return
}

func (v *Vol) extentFillDeleteOrAdd(members []*MembersFiles) {
	leader := members[0]
	store := v.store.(*storage.ExtentStore)
	deletes := store.GetDelObjects()
	sourceAddr := v.server.localServAddr
	for fileId, leaderFile := range leader.HaveFiles {
		for index := 1; index < len(members); index++ {
			follower := members[index]
			if _, ok := follower.HaveFiles[fileId]; !ok {
				addFile := &storage.ExtentInfo{Source: sourceAddr, ExtentId: fileId, Size: leaderFile.Size}
				follower.NeedAddFiles = append(follower.NeedAddFiles, addFile)
			} else {
				if leaderFile.Size > follower.HaveFiles[fileId].Size {
					fixFile := &storage.ExtentInfo{Source: sourceAddr, ExtentId: fileId, Size: leaderFile.Size}
					follower.NeedFixSizeFiles = append(follower.NeedFixSizeFiles, fixFile)
				}
			}
		}
	}
	for _, deleteFileId := range deletes {
		for index := 1; index < len(members); index++ {
			follower := members[index]
			if _, ok := follower.HaveFiles[int(deleteFileId)]; ok {
				deleteFIle := &storage.ExtentInfo{Source: sourceAddr, ExtentId: int(deleteFileId), Size: 0}
				follower.NeedDeleteFiles = append(follower.NeedDeleteFiles, deleteFIle)
			}
		}
	}
}

func (v *Vol) extentNotifyRepair(members []*MembersFiles) (err error) {
	p := NewNotifyRepair(v.volId, proto.ExtentStoreMode)
	for i := 1; i < len(members); i++ {
		var conn net.Conn
		target := v.members.VolHosts[i]
		conn, err = v.server.ConnPool.Get(target)
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
		v.server.ConnPool.Put(conn)
	}

	return
}

func (s *DataNode) repairExtents(pkg *Packet) {
	mf := NewMembersFiles()
	json.Unmarshal(pkg.Data, mf)
	store := pkg.vol.store.(*storage.ExtentStore)
	for _, deleteExtentId := range mf.NeedDeleteFiles {
		store.MarkDelete(uint64(deleteExtentId.ExtentId), 0, 0)
	}
	for _, addExtent := range mf.NeedAddFiles {
		if store.IsExsitExtent(uint64(addExtent.ExtentId)) {
			continue
		}
		err := store.Create(uint64(addExtent.ExtentId))
		if err != nil {
			continue
		}
	}

	for _, fixExtent := range mf.NeedFixSizeFiles {
		if !store.IsExsitExtent(uint64(fixExtent.ExtentId)) {
			continue
		}
		err := s.streamRepairExtent(fixExtent, pkg.vol)
		if err != nil {
			localExtentInfo, err1 := store.GetWatermark(uint64(fixExtent.ExtentId))
			if err != nil {
				err = errors.Annotatef(err1, "not exsit")
			}
			err = errors.Annotatef(err, "vol[%v] extent[%v] streamRepairExtentFailed "+
				"leaderExtentInfo[%v] localExtentInfo[%v]", fixExtent.ToString(), localExtentInfo.ToString())
			log.LogError(errors.ErrorStack(err))
		}
	}
}

func (s *DataNode) streamRepairExtent(remoteExtentInfo *storage.ExtentInfo, v *Vol) (err error) {
	store := v.store.(*storage.ExtentStore)
	if !store.IsExsitExtent(uint64(remoteExtentInfo.ExtentId)) {
		return nil
	}
	localExtentInfo, err := store.GetWatermark(uint64(remoteExtentInfo.ExtentId))
	if err != nil {
		return errors.Annotatef(err, "streamRepairExtent GetWatermark error")
	}
	needFixSize := remoteExtentInfo.Size - localExtentInfo.Size
	request := NewReadPacket(v.volId, remoteExtentInfo.ExtentId, int(localExtentInfo.Size), int(needFixSize))
	var conn net.Conn
	conn, err = s.ConnPool.Get(remoteExtentInfo.Source)
	if err != nil {
		return errors.Annotatef(err, "streamRepairExtent get conn from host[%v] error", remoteExtentInfo.Source)
	}
	err = request.WriteToConn(conn)
	if err != nil {
		conn.Close()
		return errors.Annotatef(err, "streamRepairExtent send streamRead to host[%v] error", remoteExtentInfo.Source)
	}
	for {
		localExtentInfo, err := store.GetWatermark(uint64(remoteExtentInfo.ExtentId))
		if err != nil {
			conn.Close()
			return errors.Annotatef(err, "streamRepairExtent GetWatermark error")
		}
		if localExtentInfo.Size >= remoteExtentInfo.Size {
			s.ConnPool.Put(conn)
			break
		}
		err = request.ReadFromConn(conn, proto.ReadDeadlineTime)
		if err != nil {
			conn.Close()
			return errors.Annotatef(err, "streamRepairExtent recive data error")
		}
		err = store.Write(uint64(localExtentInfo.ExtentId), int64(localExtentInfo.Size), int64(request.Size), request.Data, request.Crc)
		if err != nil {
			conn.Close()
			return errors.Annotatef(err, "streamRepairExtent repair data error")
		}
	}

}
