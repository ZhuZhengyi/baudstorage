package sdk

import (
	"encoding/json"
	"io/ioutil"
	"net"
	"net/http"
	"strings"
	"sync"
	"syscall"
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
	if err := wrapper.update(); err != nil {
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

	var inodeConn net.Conn
	var inodeCreated bool
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
			// establish the connection
			inodeConn, err = wrapper.getConn(mg)
			if err != nil {
				continue
			}
			status, inode, err = wrapper.icreate(inodeConn, mode)
			if err == nil && status == int(proto.OpOk) {
				// create inode is successful, and keep the connection
				wrapper.allocMeta <- groupid
				inodeCreated = true
				break
			}
			// break the connection
			wrapper.putConn(inodeConn, err)
		}
	}

	if !inodeCreated {
		return -1, 0, syscall.ENOMEM
	}

	status, err = wrapper.dcreate(parentConn, parentID, name, inode, mode)
	if err != nil || status != int(proto.OpOk) {
		wrapper.idelete(inodeConn, inode) //TODO: deal with error
	}
	wrapper.putConn(inodeConn, err)
	return
}

func (wrapper *MetaGroupWrapper) Lookup(parentID uint64, name string) (status int, inode uint64, mode uint32, err error) {
	_, conn, err := wrapper.connect(parentID)
	if err != nil {
		return
	}
	defer wrapper.putConn(conn, err)

	status, inode, mode, err = wrapper.lookup(conn, parentID, name)
	return
}

func (wrapper *MetaGroupWrapper) InodeGet(inode uint64) (status int, info *proto.InodeInfo, err error) {
	_, conn, err := wrapper.connect(inode)
	if err != nil {
		return
	}
	defer wrapper.putConn(conn, err)

	status, info, err = wrapper.iget(conn, inode)
	return
}

func (wrapper *MetaGroupWrapper) Delete(parentID uint64, name string) (status int, err error) {
	_, parentConn, err := wrapper.connect(parentID)
	if err != nil {
		return
	}
	defer wrapper.putConn(parentConn, err)

	status, inode, err := wrapper.ddelete(parentConn, parentID, name)
	if err != nil || status != int(proto.OpOk) {
		return
	}

	_, inodeConn, err := wrapper.connect(inode)
	if err != nil {
		return
	}
	defer wrapper.putConn(inodeConn, err)

	wrapper.idelete(inodeConn, inode) //TODO: deal with error
	return
}

func (wrapper *MetaGroupWrapper) Rename(srcParentID uint64, srcName string, dstParentID uint64, dstName string) (status int, err error) {
	_, srcParentConn, err := wrapper.connect(srcParentID)
	if err != nil {
		return
	}
	defer wrapper.putConn(srcParentConn, err)
	_, dstParentConn, err := wrapper.connect(dstParentID)
	if err != nil {
		return
	}
	defer wrapper.putConn(dstParentConn, err)

	// look up for the ino
	status, inode, mode, err := wrapper.lookup(srcParentConn, srcParentID, srcName)
	if err != nil || status != int(proto.OpOk) {
		return
	}
	// create dentry in dst parent
	status, err = wrapper.dcreate(dstParentConn, dstParentID, dstName, inode, mode)
	if err != nil || status != int(proto.OpOk) {
		return
	}
	// delete dentry from src parent
	status, _, err = wrapper.ddelete(srcParentConn, srcParentID, srcName)
	if err != nil || status != int(proto.OpOk) {
		wrapper.ddelete(dstParentConn, dstParentID, dstName) //TODO: deal with error
	}
	return
}

func (wrapper *MetaGroupWrapper) ReadDir(parentID uint64) (children []proto.Dentry, err error) {
	_, conn, err := wrapper.connect(parentID)
	if err != nil {
		return
	}
	defer wrapper.putConn(conn, err)

	children, err = wrapper.readdir(conn, parentID)
	return
}

func (wrapper *MetaGroupWrapper) icreate(conn net.Conn, mode uint32) (status int, inode uint64, err error) {
	req := &proto.CreateInodeRequest{
		Namespace: wrapper.namespace,
		Mode:      mode,
	}
	packet := proto.NewPacket()
	packet.Opcode = proto.OpMetaCreateInode
	packet.Data, err = json.Marshal(req)
	if err != nil {
		return
	}

	packet, err = wrapper.send(conn, packet)
	if err != nil {
		return
	}

	resp := new(proto.CreateInodeResponse)
	err = json.Unmarshal(packet.Data, &resp)
	if err != nil {
		return
	}
	return int(resp.Status), resp.Inode, nil
}

func (wrapper *MetaGroupWrapper) idelete(conn net.Conn, inode uint64) (status int, err error) {
	req := &proto.DeleteInodeRequest{
		Namespace: wrapper.namespace,
		Inode:     inode,
	}
	packet := proto.NewPacket()
	packet.Opcode = proto.OpMetaDeleteInode
	packet.Data, err = json.Marshal(req)
	if err != nil {
		return
	}

	packet, err = wrapper.send(conn, packet)
	if err != nil {
		return
	}

	resp := new(proto.DeleteInodeResponse)
	err = json.Unmarshal(packet.Data, &resp)
	if err != nil {
		return
	}
	return int(resp.Status), nil
}

func (wrapper *MetaGroupWrapper) dcreate(conn net.Conn, parentID uint64, name string, inode uint64, mode uint32) (status int, err error) {
	req := &proto.CreateDentryRequest{
		Namespace: wrapper.namespace,
		ParentID:  parentID,
		Inode:     inode,
		Name:      name,
		Mode:      mode,
	}
	packet := proto.NewPacket()
	packet.Opcode = proto.OpMetaCreateDentry
	packet.Data, err = json.Marshal(req)
	if err != nil {
		return
	}

	packet, err = wrapper.send(conn, packet)
	if err != nil {
		return
	}

	resp := new(proto.CreateDentryResponse)
	err = json.Unmarshal(packet.Data, &resp)
	if err != nil {
		return
	}
	return int(resp.Status), nil
}

func (wrapper *MetaGroupWrapper) ddelete(conn net.Conn, parentID uint64, name string) (status int, inode uint64, err error) {
	req := &proto.DeleteDentryRequest{
		Namespace: wrapper.namespace,
		ParentID:  parentID,
		Name:      name,
	}
	packet := proto.NewPacket()
	packet.Opcode = proto.OpMetaDeleteDentry
	packet.Data, err = json.Marshal(req)
	if err != nil {
		return
	}

	packet, err = wrapper.send(conn, packet)
	if err != nil {
		return
	}

	resp := new(proto.DeleteDentryResponse)
	err = json.Unmarshal(packet.Data, &resp)
	if err != nil {
		return
	}
	return int(resp.Status), resp.Inode, nil
}

func (wrapper *MetaGroupWrapper) lookup(conn net.Conn, parentID uint64, name string) (status int, inode uint64, mode uint32, err error) {
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
	return int(resp.Status), resp.Inode, resp.Mode, nil
}

func (wrapper *MetaGroupWrapper) iget(conn net.Conn, inode uint64) (status int, info *proto.InodeInfo, err error) {
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
	return int(resp.Status), resp.Info, nil
}

func (wrapper *MetaGroupWrapper) readdir(conn net.Conn, parentID uint64) (children []proto.Dentry, err error) {
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
			if err := wrapper.update(); err != nil {
				//TODO: log error
			}
		}
	}
}

// Update meta groups from master
func (wrapper *MetaGroupWrapper) update() error {
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
