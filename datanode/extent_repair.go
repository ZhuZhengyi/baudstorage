package datanode

import (
	"time"
	"fmt"
)

var (
	GetVolMember="/datanode/member"
)

func (v *Vol)check(){
	ticker:=time.Tick(time.Second*10)
	for {
		select {
			case <-ticker:

		}
	}
}


func (v *Vol)parseVolMember()(err error){
	var (
		data []byte
	)
	url:=fmt.Sprintf(GetVolMember+"?vol=%v",v.volId)
	data,err=v.server.PostToMaster(nil,url)
	if err!=nil {
		return
	}


}