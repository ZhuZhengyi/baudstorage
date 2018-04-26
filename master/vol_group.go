package master

import (
	"fmt"
	"github.com/tiglabs/baudstorage/proto"
	"github.com/tiglabs/baudstorage/util/log"
	"sync"
)

type Vol struct {
	addr   string
	loc    uint8
	status uint8
}

type VolGroup struct {
	VolID            uint64
	volType          string
	replicaNum       uint8
	status           uint8
	location         []*Vol
	PersistenceHosts []string
	sync.Mutex
}

func newVolGroup(volID uint64, replicaNum uint8) (vg *VolGroup) {
	vg = new(VolGroup)
	vg.replicaNum = replicaNum
	vg.VolID = volID
	vg.PersistenceHosts = make([]string, 0)
	vg.location = make([]*Vol, 0)
	return
}

func (v *VolGroup) addMember(vl *Vol) {

}

func (v *VolGroup) checkBadStatus() {

}

func (v *VolGroup) SelectHosts(c *Cluster) (err error) {
	var (
		addrs []string
	)
	v.Lock()
	defer v.Unlock()
	if addrs, err = c.getAvailHostExcludeSpecify(&v.PersistenceHosts, int(v.replicaNum)); err != nil {
		goto errDeal
	}
	v.PersistenceHosts = append(v.PersistenceHosts, addrs...)

	if len(v.PersistenceHosts) != (int)(v.replicaNum) {
		err = fmt.Errorf("no have enough volhost exsited")
		goto errDeal
	}
	log.LogDebug(fmt.Sprintf("action[SelectHosts],volID:%v,PersistenceHosts:%v",
		v.VolID, v.PersistenceHosts))
	return
errDeal:
	return
}

func (v *VolGroup) generateCreateVolGroupTasks() (tasks []*proto.AdminTask) {
	tasks = make([]*proto.AdminTask, 0)
	for _, addr := range v.PersistenceHosts {
		tasks = append(tasks, proto.NewAdminTask(OpCreateVol, addr, nil))
	}
	return
}

/*vol的某个副本磁盘坏的情况下触发vol迁移，非常重要*/
func (v *VolGroup) volOffLine(offlineAddr string, sourceFunc string) {

}
