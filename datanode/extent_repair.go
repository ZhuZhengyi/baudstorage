package datanode

import (
	"time"
	"github.com/tiglabs/baudstorage/storage"
	"github.com/tiglabs/baudstorage/proto"
	"net"
	"encoding/json"
)


func (v *Vol) check() {
	ticker := time.Tick(time.Second * 10)
	for {
		select {
		case <-ticker:

		}
	}
}


type MembersFiles struct {
	index      int
	files      map[int]*storage.FileInfo
	needDelete []*storage.FileInfo
	needAdd []*storage.FileInfo
}

func NewMembersFiles()(mf *MembersFiles){
	mf=new(MembersFiles)
	mf.files=make(map[int]*storage.FileInfo)
	mf.needDelete =make([]*storage.FileInfo,0)
	mf.needAdd=make([]*storage.FileInfo,0)
	return
}

func (v *Vol)getMembersFileIdInfo()(members []*MembersFiles,err error){
	members=make([]*MembersFiles,v.members.VolGoal)
	store:=v.store.(*storage.ExtentStore)
	mf:=NewMembersFiles()
	var files []*storage.FileInfo
	files,err=store.GetAllWatermark()
	if err!=nil {
		return
	}
	for _,fi:=range files{
		mf.files[fi.FileId]=fi
	}
	members[0]=mf
	if err!=nil {
		return
	}
	var storeMode uint8
	switch v.volMode {
	case TinyVol:
		storeMode=proto.TinyStoreMode
	case ExtentVol:
		storeMode=proto.ExtentStoreMode
	}
	p:=NewGetAllWaterMarker(v.volId,storeMode)
	for i:=1;i<len(v.members.VolHosts);i++{
		var conn net.Conn
		target:=v.members.VolHosts[i]
		conn,err=v.server.ConnPool.Get(target)
		if err!=nil {
			return
		}
		err=p.WriteToConn(conn)
		if err!=nil {
			return
		}
		err=p.ReadFromConn(conn,proto.ReadDeadlineTime)
		if err!=nil {
			return
		}
		err=json.Unmarshal(p.Data[:p.Size],members[i])
		if err!=nil {
			return
		}
		var files []*storage.FileInfo
		files,err=store.GetAllWatermark()
		if err!=nil {
			return
		}
		mf:=NewMembersFiles()
		for _,fi:=range files{
			mf.files[fi.FileId]=fi
		}
		members[i]=mf
	}
	return
}


func (v *Vol)fillNeedDeleteOrAddFiles(members []*MembersFiles){
	leader:=members[0]
	for fileId,leaderFile:=range leader.files{
		sourceAddr:=v.server.localServAddr
		for index:=1;index<len(members);index++{
			follower:=members[index]
			if _,ok:=follower.files[fileId];!ok{
				addFile:=&storage.FileInfo{Source:sourceAddr,FileId:fileId,Size:leaderFile.Size}
				follower.needAdd=append(follower.needAdd,addFile)
			}
		}
	}

}


