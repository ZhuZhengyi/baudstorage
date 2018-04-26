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
		zones []*Zone
	)
	if zones, err = c.topology.allocZone(v.replicaNum, nil); err != nil {
		return
	}
	v.Lock()
	defer v.Unlock()
	if len(zones) == 2 {
		masterZone := zones[0]
		slaveZone := zones[1]
		masterGoal := (int)(v.replicaNum)/2 + 1
		slaveGoal := (int)(v.replicaNum) - masterGoal
		if addrs, err = masterZone.getAvailHostExcludeSpecify(&v.PersistenceHosts, masterGoal); err != nil {
			goto errDeal
		}

		v.PersistenceHosts = append(v.PersistenceHosts, addrs...)
		if addrs, err = slaveZone.getAvailHostExcludeSpecify(&v.PersistenceHosts, slaveGoal); err != nil {
			goto errDeal
		}
		v.PersistenceHosts = append(v.PersistenceHosts, addrs...)
	} else if len(zones) == int(v.replicaNum) {
		for index := 0; index < int(v.replicaNum); index++ {
			zone := zones[index]
			if addrs, err = zone.getAvailHostExcludeSpecify(&v.PersistenceHosts, 1); err != nil {
				goto errDeal
			}
			v.PersistenceHosts = append(v.PersistenceHosts, addrs[0])
		}
	} else if len(zones) == 1 {
		zone := zones[0]
		if addrs, err = zone.getAvailHostExcludeSpecify(&v.PersistenceHosts, int(v.replicaNum)); err != nil {
			goto errDeal
		}
		v.PersistenceHosts = append(v.PersistenceHosts, addrs...)
	}

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
		if t, err := proto.NewAdminTask(OpCreateVol, addr, nil); err == nil {
			tasks = append(tasks, t)
		}
	}
	return
}

/*vol的某个副本磁盘坏的情况下触发vol迁移，非常重要*/
func (v *VolGroup) volOffLine(offlineAddr string, sourceFunc string) {

}
