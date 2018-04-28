package sdk

import (
	"encoding/json"
	"io/ioutil"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/google/btree"
	"github.com/juju/errors"

	"github.com/tiglabs/baudstorage/proto"
	"github.com/tiglabs/baudstorage/util/pool"
)

const (
	HostsSeparator   = ","
	MetaGroupViewURL = "/client/namespace?name="

	RefreshMetaGroupsInterval = time.Minute * 5
	CreateInodeTimeout        = time.Second * 5

	MetaAllocBufSize = 1000
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

	allocMeta chan string
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
	wrapper.allocMeta = make(chan string, MetaAllocBufSize)
	if err := wrapper.UpdateMetaGroups(); err != nil {
		return nil, err
	}
	go wrapper.refresh()
	return wrapper, nil
}

func (wrapper *MetaGroupWrapper) Create(parentID uint64, name string, mode uint32) (status int, inode uint64, err error) {
	_, parentConn, err := wrapper.connect(parentID)
	if err != nil {
		return
	}
	defer wrapper.putConn(parentConn, err)

	// Create Inode
	for {
		// Reset timer for each select
		t := time.NewTicker(CreateInodeTimeout)
		select {
		case <-t.C:
			break
		case groupid := <-wrapper.allocMeta:
			mg := wrapper.getMetaGroupByID(groupid)
			if mg == nil {
				continue
			}
			inodeConn, err := wrapper.getConn(mg)
			if err != nil {
				continue
			}
			status, inode, err = wrapper.icreate(inodeConn)
			if err == nil && status == int(proto.OpOk) {
				// create inode is successful
				wrapper.allocMeta <- groupid
				break
			}
		}
	}
	//TODO: create a dentry in the parent inode
	return
}

func (wrapper *MetaGroupWrapper) icreate(conn net.Conn) (status int, inode uint64, err error) {
	return
}

func (wrapper *MetaGroupWrapper) Lookup(parentID uint64, name string) (status int, inode uint64, mode uint32, err error) {
	_, conn, err := wrapper.connect(parentID)
	if err != nil {
		return
	}
	defer wrapper.putConn(conn, err)

	status, inode, mode, err = wrapper.lookup(parentID, name, conn)
	return
}

func (wrapper *MetaGroupWrapper) lookup(parentID uint64, name string, conn net.Conn) (status int, inode uint64, mode uint32, err error) {
	req := &proto.LookupRequest{
		Namespace: wrapper.namespace,
		ParentID:  parentID,
		Name:      name,
	}
	packet := proto.NewPacket()
	packet.Opcode = proto.OpMetaLookup
	packet.Data, err = json.Marshal(req)
	if err != nil {
		return
	}

	packet, err = wrapper.send(conn, packet)
	if err != nil {
		return
	}

	resp := new(proto.LookupResponse)
	err = json.Unmarshal(packet.Data, &resp)
	if err != nil {
		return
	}
	return resp.Status, resp.Inode, resp.Mode, nil
}

func (wrapper *MetaGroupWrapper) InodeGet(inode uint64) (status int, info *proto.InodeInfo, err error) {
	_, conn, err := wrapper.connect(inode)
	if err != nil {
		return
	}
	defer wrapper.putConn(conn, err)

	status, info, err = wrapper.iget(inode, conn)
	return
}

func (wrapper *MetaGroupWrapper) iget(inode uint64, conn net.Conn) (status int, info *proto.InodeInfo, err error) {
	req := &proto.InodeGetRequest{
		Namespace: wrapper.namespace,
		Inode:     inode,
	}
	packet := proto.NewPacket()
	packet.Opcode = proto.OpMetaInodeGet
	packet.Data, err = json.Marshal(req)
	if err != nil {
		return
	}

	packet, err = wrapper.send(conn, packet)
	if err != nil {
		return
	}

	resp := new(proto.InodeGetResponse)
	err = json.Unmarshal(packet.Data, &resp)
	if err != nil {
		return
	}
	return resp.Status, resp.Info, nil
}

func (wrapper *MetaGroupWrapper) Delete(parent uint64, name string) (status int, err error) {
	//TODO: delete dentry in parent inode
	//TODO: delete inode
	return
}

func (wrapper *MetaGroupWrapper) Rename(srcParent uint64, srcName string, dstParent uint64, dstName string) (status int, err error) {
	//TODO: delete dentry from src parent
	//TODO: create dentry in dst parent
	//TODO: update inode name
	//TODO: commit the change
	return
}

func (wrapper *MetaGroupWrapper) ReadDir(parentID uint64) (children []proto.Dentry, err error) {
	_, conn, err := wrapper.connect(parentID)
	if err != nil {
		return
	}
	defer wrapper.putConn(conn, err)

	children, err = wrapper.readdir(parentID, conn)
	return
}

func (wrapper *MetaGroupWrapper) readdir(parentID uint64, conn net.Conn) (children []proto.Dentry, err error) {
	req := &proto.ReadDirRequest{
		Namespace: wrapper.namespace,
		ParentID:  parentID,
	}
	packet := proto.NewPacket()
	packet.Opcode = proto.OpMetaReadDir
	packet.Data, err = json.Marshal(req)
	if err != nil {
		return
	}

	packet, err = wrapper.send(conn, packet)
	if err != nil {
		return
	}

	resp := new(proto.ReadDirResponse)
	err = json.Unmarshal(packet.Data, &resp)
	if err != nil {
		return
	}
	return resp.Children, nil
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
		//TODO: if the meta group is full, do not put into the channel
		select {
		case wrapper.allocMeta <- mg.GroupID:
		default:
		}
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

	//TODO: if mg is nil, update meta groups and try again

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

func (wrapper *MetaGroupWrapper) getConn(mg *MetaGroup) (net.Conn, error) {
	addr := mg.Members[0]
	//TODO: deal with member 0 is not leader
	return wrapper.conns.Get(addr)
}

func (wrapper *MetaGroupWrapper) putConn(conn net.Conn, err error) {
	if err != nil {
		conn.Close()
	} else {
		wrapper.conns.Put(conn)
	}
}

func (wrapper *MetaGroupWrapper) connect(inode uint64) (*MetaGroup, net.Conn, error) {
	mg := wrapper.getMetaGroupByInode(inode)
	if mg == nil {
		return nil, nil, errors.New("No such meta group")
	}
	conn, err := wrapper.getConn(mg)
	if err != nil {
		return nil, nil, err
	}
	return mg, conn, nil
}

func (wrapper *MetaGroupWrapper) send(conn net.Conn, req *proto.Packet) (*proto.Packet, error) {
	err := req.WriteToConn(conn)
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
