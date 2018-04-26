package sdk

import (
	"encoding/json"
	"github.com/google/btree"
	"io/ioutil"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/tiglabs/baudstorage/util/pool"
)

const (
	HostsSeparator   = ","
	MetaGroupViewURL = "/client/namespace?name="

	RefreshMetaGroupsInterval = time.Minute * 5
)

type MetaGroup struct {
	GroupID string
	Start   uint64
	End     uint64
	Members []string
}

type NamespaceView struct {
	Name       string
	MetaGroups []*MetaGroup
}

type MetaGroupWrapper struct {
	namespace string
	master    []string
	conns     *pool.ConnPool

	// groups and ranges should be modified together.
	// do not use groups and ranges directly, use the helper functions instead.
	groups map[string]*MetaGroup
	ranges *btree.BTree // *MetaGroup tree indexed by Start

	sync.RWMutex
}

func (this *MetaGroup) Less(than btree.Item) bool {
	that := than.(*MetaGroup)
	return this.Start < that.Start
}

func NewMetaGroupWrapper(namespace, masterHosts string) (*MetaGroupWrapper, error) {
	wrapper := new(MetaGroupWrapper)
	wrapper.master = strings.Split(masterHosts, HostsSeparator)
	wrapper.conns = pool.NewConnPool()
	wrapper.groups = make(map[string]*MetaGroup)
	wrapper.ranges = btree.New(32)
	if err := wrapper.UpdateMetaGroups(); err != nil {
		return nil, err
	}
	go wrapper.refresh()
	return wrapper, nil
}

func (wrapper *MetaGroupWrapper) refresh() {
	t := time.NewTicker(RefreshMetaGroupsInterval)
	for {
		select {
		case <-t.C:
			if err := wrapper.UpdateMetaGroups(); err != nil {
				//TODO: log error
			}
		}
	}
}

func (wrapper *MetaGroupWrapper) UpdateMetaGroups() error {
	nv, err := wrapper.GetNamespaceView()
	if err != nil {
		return err
	}

	for _, mg := range nv.MetaGroups {
		wrapper.InsertOrUpdateMetaGroup(mg)
	}

	return nil
}

func (wrapper *MetaGroupWrapper) InsertOrUpdateMetaGroup(mg *MetaGroup) {
	wrapper.Lock()
	defer wrapper.Unlock()

	found, ok := wrapper.groups[mg.GroupID]
	if ok {
		wrapper.deleteMetaGroup(found)
	}

	wrapper.addMetaGroup(mg)
	return
}

func (wrapper *MetaGroupWrapper) GetMetaGroupByID(id string) *MetaGroup {
	wrapper.RLock()
	defer wrapper.RUnlock()
	mg, ok := wrapper.groups[id]
	if !ok {
		return nil
	}
	return mg
}

func (wrapper *MetaGroupWrapper) GetMetaGroupByInode(ino uint64) *MetaGroup {
	var mg *MetaGroup
	wrapper.RLock()
	defer wrapper.RUnlock()

	pivot := &MetaGroup{Start: ino}
	wrapper.ranges.DescendLessOrEqual(pivot, func(i btree.Item) bool {
		mg = i.(*MetaGroup)
		if ino > mg.End || ino < mg.Start {
			mg = nil
		}
		// Iterate one item is enough
		return false
	})

	return mg
}

func (wrapper *MetaGroupWrapper) addMetaGroup(mg *MetaGroup) {
	wrapper.groups[mg.GroupID] = mg
	wrapper.ranges.ReplaceOrInsert(mg)
}

func (wrapper *MetaGroupWrapper) deleteMetaGroup(mg *MetaGroup) {
	delete(wrapper.groups, mg.GroupID)
	wrapper.ranges.Delete(mg)
}

func (wrapper *MetaGroupWrapper) GetNamespaceView() (*NamespaceView, error) {
	addr := wrapper.master[0]
	resp, err := http.Get("http://" + addr + MetaGroupViewURL + wrapper.namespace)
	if err != nil {
		//TODO: master would return the leader addr if it is a follower
		return nil, err
	}
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		//TODO: log
		return nil, err
	}

	view := new(NamespaceView)
	if err = json.Unmarshal(body, &view); err != nil {
		//TODO: log
		return nil, err
	}

	return view, nil
}
