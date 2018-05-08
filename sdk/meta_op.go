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
	HostsSeparator       = ","
	MetaPartitionViewURL = "/client/namespace?name="

	RefreshMetaPartitionsInterval = time.Minute * 5
	CreateInodeTimeout            = time.Second * 5

	MetaAllocBufSize = 1000
)

type MetaPartition struct {
	GroupID string
	Start   uint64
	End     uint64
	Members []string
}

type NamespaceView struct {
	Name           string
	MetaPartitions []*MetaPartition
}

type MetaWrapper struct {
	namespace string
	master    []string
	conns     *pool.ConnPool

	// partitions and ranges should be modified together.
	// do not use partitions and ranges directly, use the helper functions instead.
	partitions map[string]*MetaPartition
	ranges     *btree.BTree // *MetaPartition tree indexed by Start

	allocMeta chan string
	sync.RWMutex
}

func (this *MetaPartition) Less(than btree.Item) bool {
	that := than.(*MetaPartition)
	return this.Start < that.Start
}

func NewMetaWrapper(namespace, masterHosts string) (*MetaWrapper, error) {
	mw := new(MetaWrapper)
	mw.master = strings.Split(masterHosts, HostsSeparator)
	mw.conns = pool.NewConnPool()
	mw.partitions = make(map[string]*MetaPartition)
	mw.ranges = btree.New(32)
	mw.allocMeta = make(chan string, MetaAllocBufSize)
	if err := mw.update(); err != nil {
		return nil, err
	}
	go mw.refresh()
	return mw, nil
}

func (mw *MetaWrapper) icreate(conn net.Conn, mode uint32) (status int, info *proto.InodeInfo, err error) {
	req := &proto.CreateInodeRequest{
		Namespace: mw.namespace,
		Mode:      mode,
	}
	packet := proto.NewPacket()
	packet.Opcode = proto.OpMetaCreateInode
	packet.Data, err = json.Marshal(req)
	if err != nil {
		return
	}

	packet, err = mw.send(conn, packet)
	if err != nil {
		return
	}

	resp := new(proto.CreateInodeResponse)
	err = json.Unmarshal(packet.Data, &resp)
	if err != nil {
		return
	}
	return int(resp.Status), resp.Info, nil
}

func (mw *MetaWrapper) idelete(conn net.Conn, inode uint64) (status int, err error) {
	req := &proto.DeleteInodeRequest{
		Namespace: mw.namespace,
		Inode:     inode,
	}
	packet := proto.NewPacket()
	packet.Opcode = proto.OpMetaDeleteInode
	packet.Data, err = json.Marshal(req)
	if err != nil {
		return
	}

	packet, err = mw.send(conn, packet)
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

func (mw *MetaWrapper) dcreate(conn net.Conn, parentID uint64, name string, inode uint64, mode uint32) (status int, err error) {
	req := &proto.CreateDentryRequest{
		Namespace: mw.namespace,
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

	packet, err = mw.send(conn, packet)
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

func (mw *MetaWrapper) ddelete(conn net.Conn, parentID uint64, name string) (status int, inode uint64, err error) {
	req := &proto.DeleteDentryRequest{
		Namespace: mw.namespace,
		ParentID:  parentID,
		Name:      name,
	}
	packet := proto.NewPacket()
	packet.Opcode = proto.OpMetaDeleteDentry
	packet.Data, err = json.Marshal(req)
	if err != nil {
		return
	}

	packet, err = mw.send(conn, packet)
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

func (mw *MetaWrapper) lookup(conn net.Conn, parentID uint64, name string) (status int, inode uint64, mode uint32, err error) {
	req := &proto.LookupRequest{
		Namespace: mw.namespace,
		ParentID:  parentID,
		Name:      name,
	}
	packet := proto.NewPacket()
	packet.Opcode = proto.OpMetaLookup
	packet.Data, err = json.Marshal(req)
	if err != nil {
		return
	}

	packet, err = mw.send(conn, packet)
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

func (mw *MetaWrapper) iget(conn net.Conn, inode uint64) (status int, info *proto.InodeInfo, err error) {
	req := &proto.InodeGetRequest{
		Namespace: mw.namespace,
		Inode:     inode,
	}
	packet := proto.NewPacket()
	packet.Opcode = proto.OpMetaInodeGet
	packet.Data, err = json.Marshal(req)
	if err != nil {
		return
	}

	packet, err = mw.send(conn, packet)
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

func (mw *MetaWrapper) readdir(conn net.Conn, parentID uint64) (children []proto.Dentry, err error) {
	req := &proto.ReadDirRequest{
		Namespace: mw.namespace,
		ParentID:  parentID,
	}
	packet := proto.NewPacket()
	packet.Opcode = proto.OpMetaReadDir
	packet.Data, err = json.Marshal(req)
	if err != nil {
		return
	}

	packet, err = mw.send(conn, packet)
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

func (mw *MetaWrapper) refresh() {
	t := time.NewTicker(RefreshMetaPartitionsInterval)
	for {
		select {
		case <-t.C:
			if err := mw.update(); err != nil {
				//TODO: log error
			}
		}
	}
}

// Update meta partitions from master
func (mw *MetaWrapper) update() error {
	nv, err := mw.getNamespaceView()
	if err != nil {
		return err
	}

	for _, mp := range nv.MetaPartitions {
		mw.replaceOrInsertMetaPartition(mp)
		//TODO: if the meta group is full, do not put into the channel
		select {
		case mw.allocMeta <- mp.GroupID:
		default:
		}
	}

	return nil
}

func (mw *MetaWrapper) replaceOrInsertMetaPartition(mp *MetaPartition) {
	mw.Lock()
	defer mw.Unlock()

	found, ok := mw.partitions[mp.GroupID]
	if ok {
		mw.deleteMetaPartition(found)
	}

	mw.addMetaPartition(mp)
	return
}

func (mw *MetaWrapper) getMetaPartitionByID(id string) *MetaPartition {
	mw.RLock()
	defer mw.RUnlock()
	mp, ok := mw.partitions[id]
	if !ok {
		return nil
	}
	return mp
}

func (mw *MetaWrapper) getMetaPartitionByInode(ino uint64) *MetaPartition {
	var mp *MetaPartition
	mw.RLock()
	defer mw.RUnlock()

	pivot := &MetaPartition{Start: ino}
	mw.ranges.DescendLessOrEqual(pivot, func(i btree.Item) bool {
		mp = i.(*MetaPartition)
		if ino > mp.End || ino < mp.Start {
			mp = nil
		}
		// Iterate one item is enough
		return false
	})

	//TODO: if mp is nil, update meta partitions and try again

	return mp
}

func (mw *MetaWrapper) addMetaPartition(mp *MetaPartition) {
	mw.partitions[mp.GroupID] = mp
	mw.ranges.ReplaceOrInsert(mp)
}

func (mw *MetaWrapper) deleteMetaPartition(mp *MetaPartition) {
	delete(mw.partitions, mp.GroupID)
	mw.ranges.Delete(mp)
}

func (mw *MetaWrapper) getNamespaceView() (*NamespaceView, error) {
	addr := mw.master[0]
	resp, err := http.Get("http://" + addr + MetaPartitionViewURL + mw.namespace)
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

func (mw *MetaWrapper) getConn(mp *MetaPartition) (net.Conn, error) {
	addr := mp.Members[0]
	//TODO: deal with member 0 is not leader
	return mw.conns.Get(addr)
}

func (mw *MetaWrapper) putConn(conn net.Conn, err error) {
	if err != nil {
		conn.Close()
	} else {
		mw.conns.Put(conn)
	}
}

func (mw *MetaWrapper) connect(inode uint64) (*MetaPartition, net.Conn, error) {
	mp := mw.getMetaPartitionByInode(inode)
	if mp == nil {
		return nil, nil, errors.New("No such meta group")
	}
	conn, err := mw.getConn(mp)
	if err != nil {
		return nil, nil, err
	}
	return mp, conn, nil
}

func (mw *MetaWrapper) send(conn net.Conn, req *proto.Packet) (*proto.Packet, error) {
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
