package sdk

import (
	"encoding/json"
	"github.com/google/btree"
	"github.com/juju/errors"
	"io/ioutil"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/tiglabs/baudstorage/proto"
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

func (wrapper *MetaGroupWrapper) Create(req *proto.CreateRequest) (*proto.CreateResponse, error) {
	resp := new(proto.CreateResponse)
	return resp, nil
}

func (wrapper *MetaGroupWrapper) Lookup(req *proto.LookupRequest) (*proto.LookupResponse, error) {
	resp := new(proto.LookupResponse)
	return resp, nil
}

func (wrapper *MetaGroupWrapper) InodeGet(req *proto.InodeGetRequest) (*proto.InodeGetResponse, error) {
	resp := new(proto.InodeGetResponse)
	return resp, nil
}

func (wrapper *MetaGroupWrapper) Delete(req *proto.DeleteRequest) (*proto.DeleteResponse, error) {
	resp := new(proto.DeleteResponse)
	return resp, nil
}

func (wrapper *MetaGroupWrapper) Rename(req *proto.RenameRequest) (*proto.RenameResponse, error) {
	resp := new(proto.RenameResponse)
	return resp, nil
}

func (wrapper *MetaGroupWrapper) ReadDir(req *proto.ReadDirRequest) (*proto.ReadDirResponse, error) {
	resp := new(proto.ReadDirResponse)
	return resp, nil
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
	nv, err := wrapper.getNamespaceView()
	if err != nil {
		return err
	}

	for _, mg := range nv.MetaGroups {
		wrapper.replaceOrInsertMetaGroup(mg)
	}

	return nil
}

func (wrapper *MetaGroupWrapper) replaceOrInsertMetaGroup(mg *MetaGroup) {
	wrapper.Lock()
	defer wrapper.Unlock()

	found, ok := wrapper.groups[mg.GroupID]
	if ok {
		wrapper.deleteMetaGroup(found)
	}

	wrapper.addMetaGroup(mg)
	return
}

func (wrapper *MetaGroupWrapper) getMetaGroupByID(id string) *MetaGroup {
	wrapper.RLock()
	defer wrapper.RUnlock()
	mg, ok := wrapper.groups[id]
	if !ok {
		return nil
	}
	return mg
}

func (wrapper *MetaGroupWrapper) getMetaGroupByInode(ino uint64) *MetaGroup {
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

func (wrapper *MetaGroupWrapper) getNamespaceView() (*NamespaceView, error) {
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

func (wrapper *MetaGroupWrapper) getConn(addr string) (net.Conn, error) {
	return wrapper.conns.Get(addr)
}

func (wrapper *MetaGroupWrapper) putConn(conn net.Conn) {
	wrapper.conns.Put(conn)
}

func (wrapper *MetaGroupWrapper) send(ino uint64, req *proto.Packet) (*proto.Packet, error) {
	mg := wrapper.getMetaGroupByInode(ino)
	if mg == nil {
		return nil, errors.New("No such meta group")
	}

	conn, err := wrapper.getConn(mg.Members[0])
	if err != nil {
		return nil, err
	}

	defer func() {
		if err != nil {
			conn.Close()
		} else {
			wrapper.putConn(conn)
		}
	}()

	err = req.WriteToConn(conn)
	if err != nil {
		return nil, err
	}

	resp := proto.NewPacket()
	err = resp.ReadFromConn(conn, proto.ReadDeadlineTime)
	if err != nil {
		return nil, err
	}

	return resp, nil
}
