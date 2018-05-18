package datanode

import (
	"encoding/json"
	"github.com/tiglabs/baudstorage/proto"
	"github.com/tiglabs/baudstorage/storage"
	"net"
	"time"
	"github.com/juju/errors"
	"github.com/tiglabs/baudstorage/util/log"
)

func (v *Vol) check() {
	ticker := time.Tick(time.Second * 100)
	for {
		select {
		case <-ticker:
			v.extentRepair()
		}
	}
}

type MembersFiles struct {
	Index           int
	HaveFiles       map[int]*storage.ExtentInfo
	NeedDeleteFiles []*storage.ExtentInfo
	NeedAddFiles    []*storage.ExtentInfo
	NeedFixSizeFiles []*storage.ExtentInfo
}


func NewMembersFiles() (mf *MembersFiles) {
	mf = new(MembersFiles)
	mf.HaveFiles = make(map[int]*storage.ExtentInfo)
	mf.NeedDeleteFiles = make([]*storage.ExtentInfo, 0)
	mf.NeedAddFiles = make([]*storage.ExtentInfo, 0)
	return
}

func (v *Vol)extentRepair(){
	members,err:=v.extentGetMembersExtents()
	if err!=nil {
		log.LogError(errors.ErrorStack(err))
		return
	}
	v.extentFillDeleteOrAdd(members)
	err=v.extentNotifyRepair(members)
	if err!=nil {
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
		err=errors.Annotatef(err,"extentGetMembersExtents vol[%v] GetAllWaterMark",v.volId)
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
			err=errors.Annotatef(err,"extentGetMembersExtents  vol[%v] get host[%v] connect",v.volId,target)
			return
		}
		err = p.WriteToConn(conn)
		if err != nil {
			conn.Close()
			err=errors.Annotatef(err,"extentGetMembersExtents vol[%v] write to host[%v]",v.volId,target)
			return
		}
		err = p.ReadFromConn(conn, proto.ReadDeadlineTime)
		if err != nil {
			conn.Close()
			err=errors.Annotatef(err,"extentGetMembersExtents vol[%v] read from host[%v]",v.volId,target)
			return
		}
		mf := NewMembersFiles()
		err = json.Unmarshal(p.Data[:p.Size], mf)
		if err != nil {
			v.server.ConnPool.Put(conn)
			err=errors.Annotatef(err,"extentGetMembersExtents json unmarsh [%v]",v.volId,string(p.Data[:p.Size]))
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
	store:=v.store.(*storage.ExtentStore)
	deletes:=store.GetDelObjects()
	sourceAddr := v.server.localServAddr
	for fileId, leaderFile := range leader.HaveFiles {
		for index := 1; index < len(members); index++ {
			follower := members[index]
			if _, ok := follower.HaveFiles[fileId]; !ok {
				addFile := &storage.ExtentInfo{Source: sourceAddr, ExtentId: fileId, Size: leaderFile.Size}
				follower.NeedAddFiles = append(follower.NeedAddFiles, addFile)
			}else {
				if leaderFile.Size>follower.HaveFiles[fileId].Size{
					fixFile:= &storage.ExtentInfo{Source: sourceAddr, ExtentId: fileId, Size: leaderFile.Size}
					follower.NeedFixSizeFiles=append(follower.NeedFixSizeFiles,fixFile)
				}
			}
		}
	}
	for _,deleteFileId:=range deletes{
		for index := 1; index < len(members); index++ {
			follower := members[index]
			if _, ok := follower.HaveFiles[int(deleteFileId)]; ok {
				deleteFIle := &storage.ExtentInfo{Source: sourceAddr, ExtentId: int(deleteFileId), Size:0}
				follower.NeedDeleteFiles = append(follower.NeedDeleteFiles, deleteFIle)
			}
		}
	}
}


func (v *Vol) extentNotifyRepair(members []*MembersFiles)(err error){
	p:=NewNotifyRepair(v.volId,proto.ExtentStoreMode)
	for i:=1;i<len(members);i++{
		var conn net.Conn
		target := v.members.VolHosts[i]
		conn, err = v.server.ConnPool.Get(target)
		if err != nil {
			continue
		}
		p.Data,err=json.Marshal(members[i])
		p.Size=uint32(len(p.Data))
		err=p.WriteToConn(conn)
		if err!=nil {
			conn.Close()
			continue
		}
		v.server.ConnPool.Put(conn)
	}

	return
}


