package meta

import (
	"encoding/json"
	"io"
	"io/ioutil"
	"log"
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

const (
	statusOK int = iota
	statusExist
	statusNoent
	statusFull
	statusUnknownError
)

type MetaPartition struct {
	PartitionID uint64
	Start       uint64
	End         uint64
	Members     []string
	LeaderAddr  string
}

type MetaConn struct {
	conn net.Conn
	id   uint64 //PartitionID
}

type NamespaceView struct {
	Name           string
	MetaPartitions []*MetaPartition
}

type MetaWrapper struct {
	sync.RWMutex
	namespace string
	master    []string
	conns     *pool.ConnPool

	// partitions and ranges should be modified together.
	// do not use partitions and ranges directly, use the helper functions instead.
	partitions map[uint64]*MetaPartition
	ranges     *btree.BTree // *MetaPartition tree indexed by Start

	currStart uint64
}

func (this *MetaPartition) Less(than btree.Item) bool {
	that := than.(*MetaPartition)
	return this.Start < that.Start
}

func init() {
	log.SetFlags(log.Ldate | log.Ltime | log.Lmicroseconds | log.Lshortfile)
}

func NewMetaWrapper(namespace, masterHosts string) (*MetaWrapper, error) {
	mw := new(MetaWrapper)
	mw.namespace = namespace
	mw.master = strings.Split(masterHosts, HostsSeparator)
	mw.conns = pool.NewConnPool()
	mw.partitions = make(map[uint64]*MetaPartition)
	mw.ranges = btree.New(32)
	if err := mw.Update(); err != nil {
		return nil, err
	}
	mw.currStart = proto.ROOT_INO
	go mw.refresh()
	return mw, nil
}

// Namespace view managements
//

func (mw *MetaWrapper) PullNamespaceView() (*NamespaceView, error) {
	addr := mw.master[0]
	resp, err := http.Get("http://" + addr + MetaPartitionViewURL + mw.namespace)
	if err != nil {
		return nil, err
	}

	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		//TODO: master would return the leader addr if it is a follower
		log.Printf("StatusCode(%v)", resp.StatusCode)
		err = errors.New("Get namespace view failed!")
		return nil, err
	}

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		//TODO: log
		return nil, err
	}

	view := new(NamespaceView)
	if err = json.Unmarshal(body, view); err != nil {
		//TODO: log
		return nil, err
	}

	return view, nil
}

func (mw *MetaWrapper) Update() error {
	nv, err := mw.PullNamespaceView()
	if err != nil {
		return err
	}

	for _, mp := range nv.MetaPartitions {
		mw.replaceOrInsertPartition(mp)
	}
	return nil
}

func (mw *MetaWrapper) refresh() {
	t := time.NewTicker(RefreshMetaPartitionsInterval)
	for {
		select {
		case <-t.C:
			if err := mw.Update(); err != nil {
				//TODO: log error
			}
		}
	}
}

// Meta partition managements
//

func (mw *MetaWrapper) addPartition(mp *MetaPartition) {
	mw.partitions[mp.PartitionID] = mp
	mw.ranges.ReplaceOrInsert(mp)
}

func (mw *MetaWrapper) deletePartition(mp *MetaPartition) {
	delete(mw.partitions, mp.PartitionID)
	mw.ranges.Delete(mp)
}

func (mw *MetaWrapper) replaceOrInsertPartition(mp *MetaPartition) {
	mw.Lock()
	defer mw.Unlock()

	found, ok := mw.partitions[mp.PartitionID]
	if ok {
		mw.deletePartition(found)
	}

	mw.addPartition(mp)
	return
}

func (mw *MetaWrapper) getPartitionByID(id uint64) *MetaPartition {
	mw.RLock()
	defer mw.RUnlock()
	mp, ok := mw.partitions[id]
	if !ok {
		return nil
	}
	return mp
}

func (mw *MetaWrapper) getPartitionByInode(ino uint64) *MetaPartition {
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

	return mp
}

// Get the partition whose Start is Larger than ino.
// Return nil if no successive partition.
func (mw *MetaWrapper) getNextPartition(ino uint64) *MetaPartition {
	var mp *MetaPartition
	mw.RLock()
	defer mw.RUnlock()

	pivot := &MetaPartition{Start: ino + 1}
	mw.ranges.AscendGreaterOrEqual(pivot, func(i btree.Item) bool {
		mp = i.(*MetaPartition)
		return false
	})

	return mp
}

// Connection managements
//

func (mw *MetaWrapper) getConn(mp *MetaPartition) (*MetaConn, error) {
	addr := mp.LeaderAddr
	//TODO: deal with member 0 is not leader
	conn, err := mw.conns.Get(addr)
	if err != nil {
		return nil, err
	}

	mc := &MetaConn{conn: conn, id: mp.PartitionID}
	return mc, nil
}

func (mw *MetaWrapper) putConn(mc *MetaConn, err error) {
	if err != nil {
		mc.conn.Close()
	} else {
		mw.conns.Put(mc.conn)
	}
}

func (mw *MetaWrapper) connect(inode uint64) (*MetaConn, error) {
	mp := mw.getPartitionByInode(inode)
	if mp == nil {
		return nil, errors.New("No such meta group")
	}
	mc, err := mw.getConn(mp)
	if err != nil {
		return nil, err
	}
	return mc, nil
}

func (mc *MetaConn) send(req *proto.Packet) (*proto.Packet, error) {
	err := req.WriteToConn(mc.conn)
	if err != nil {
		return nil, err
	}
	resp := proto.NewPacket()
	err = resp.ReadFromConn(mc.conn, proto.ReadDeadlineTime)
	if err != nil && err != io.EOF {
		return nil, err
	}
	return resp, nil
}

// Status code conversion
func parseStatus(status uint8) (ret int) {
	switch status {
	case proto.OpOk:
		ret = statusOK
	case proto.OpExistErr:
		ret = statusExist
	case proto.OpNotExistErr:
		ret = statusNoent
	case proto.OpInodeFullErr:
		ret = statusFull
	default:
		ret = statusUnknownError
	}
	return
}

// API implementations
//

func (mw *MetaWrapper) icreate(mc *MetaConn, mode uint32) (status int, info *proto.InodeInfo, err error) {
	req := &proto.CreateInodeRequest{
		Namespace:   mw.namespace,
		PartitionID: mc.id,
		Mode:        mode,
	}

	packet := proto.NewPacket()
	packet.Opcode = proto.OpMetaCreateInode
	err = packet.MarshalData(req)
	if err != nil {
		log.Println(err)
		return
	}

	packet, err = mc.send(packet)
	if err != nil {
		log.Println(err)
		return
	}

	status = parseStatus(packet.ResultCode)
	if status != statusOK {
		return
	}

	resp := new(proto.CreateInodeResponse)
	err = packet.UnmarshalData(resp)
	if err != nil {
		log.Println(err)
		log.Printf("data = [%v]\n", string(packet.Data))
		return
	}
	return statusOK, resp.Info, nil
}

func (mw *MetaWrapper) idelete(mc *MetaConn, inode uint64) (status int, err error) {
	req := &proto.DeleteInodeRequest{
		Namespace:   mw.namespace,
		PartitionID: mc.id,
		Inode:       inode,
	}

	packet := proto.NewPacket()
	packet.Opcode = proto.OpMetaDeleteInode
	err = packet.MarshalData(req)
	if err != nil {
		return
	}

	packet, err = mc.send(packet)
	if err != nil {
		return
	}
	return parseStatus(packet.ResultCode), nil
}

func (mw *MetaWrapper) dcreate(mc *MetaConn, parentID uint64, name string, inode uint64, mode uint32) (status int, err error) {
	req := &proto.CreateDentryRequest{
		Namespace:   mw.namespace,
		PartitionID: mc.id,
		ParentID:    parentID,
		Inode:       inode,
		Name:        name,
		Mode:        mode,
	}

	packet := proto.NewPacket()
	packet.Opcode = proto.OpMetaCreateDentry
	err = packet.MarshalData(req)
	if err != nil {
		return
	}

	packet, err = mc.send(packet)
	if err != nil {
		return
	}
	return parseStatus(packet.ResultCode), nil
}

func (mw *MetaWrapper) ddelete(mc *MetaConn, parentID uint64, name string) (status int, inode uint64, err error) {
	req := &proto.DeleteDentryRequest{
		Namespace:   mw.namespace,
		PartitionID: mc.id,
		ParentID:    parentID,
		Name:        name,
	}

	packet := proto.NewPacket()
	packet.Opcode = proto.OpMetaDeleteDentry
	err = packet.MarshalData(req)
	if err != nil {
		return
	}

	packet, err = mc.send(packet)
	if err != nil {
		return
	}

	status = parseStatus(packet.ResultCode)
	if status != statusOK {
		return
	}

	resp := new(proto.DeleteDentryResponse)
	err = packet.UnmarshalData(resp)
	if err != nil {
		return
	}
	return statusOK, resp.Inode, nil
}

func (mw *MetaWrapper) lookup(mc *MetaConn, parentID uint64, name string) (status int, inode uint64, mode uint32, err error) {
	req := &proto.LookupRequest{
		Namespace:   mw.namespace,
		PartitionID: mc.id,
		ParentID:    parentID,
		Name:        name,
	}
	packet := proto.NewPacket()
	packet.Opcode = proto.OpMetaLookup
	err = packet.MarshalData(req)
	if err != nil {
		return
	}

	packet, err = mc.send(packet)
	if err != nil {
		return
	}

	status = parseStatus(packet.ResultCode)
	if status != statusOK {
		return
	}

	resp := new(proto.LookupResponse)
	err = packet.UnmarshalData(resp)
	if err != nil {
		return
	}
	return statusOK, resp.Inode, resp.Mode, nil
}

func (mw *MetaWrapper) iget(mc *MetaConn, inode uint64) (status int, info *proto.InodeInfo, err error) {
	req := &proto.InodeGetRequest{
		Namespace:   mw.namespace,
		PartitionID: mc.id,
		Inode:       inode,
	}

	packet := proto.NewPacket()
	packet.Opcode = proto.OpMetaInodeGet
	err = packet.MarshalData(req)
	if err != nil {
		log.Println(err)
		return
	}

	packet, err = mc.send(packet)
	if err != nil {
		log.Println(err)
		return
	}

	status = parseStatus(packet.ResultCode)
	if status != statusOK {
		return
	}

	resp := new(proto.InodeGetResponse)
	err = packet.UnmarshalData(resp)
	if err != nil {
		log.Println(err)
		return
	}
	return statusOK, resp.Info, nil
}

func (mw *MetaWrapper) readdir(mc *MetaConn, parentID uint64) (status int, children []proto.Dentry, err error) {
	req := &proto.ReadDirRequest{
		Namespace:   mw.namespace,
		PartitionID: mc.id,
		ParentID:    parentID,
	}

	packet := proto.NewPacket()
	packet.Opcode = proto.OpMetaReadDir
	err = packet.MarshalData(req)
	if err != nil {
		return
	}

	packet, err = mc.send(packet)
	if err != nil {
		return
	}

	status = parseStatus(packet.ResultCode)
	if status != statusOK {
		children = make([]proto.Dentry, 0)
		return
	}

	resp := new(proto.ReadDirResponse)
	err = packet.UnmarshalData(resp)
	if err != nil {
		return
	}
	return statusOK, resp.Children, nil
}

func (mw *MetaWrapper) appendExtentKey(mc *MetaConn, inode uint64, extent proto.ExtentKey) (status int, err error) {
	req := &proto.AppendExtentKeyRequest{
		Namespace:   mw.namespace,
		PartitionID: mc.id,
		Inode:       inode,
		Extent:      extent,
	}

	packet := proto.NewPacket()
	packet.Opcode = proto.OpMetaExtentsAdd
	err = packet.MarshalData(req)
	if err != nil {
		log.Println(err)
		return
	}

	packet, err = mc.send(packet)
	if err != nil {
		log.Println(err)
		return
	}
	if packet.ResultCode != proto.OpOk {
		log.Printf("ResultCode(%v)\n", packet.ResultCode)
	}
	return parseStatus(packet.ResultCode), nil
}

func (mw *MetaWrapper) getExtents(mc *MetaConn, inode uint64) (status int, extents []proto.ExtentKey, err error) {
	req := &proto.GetExtentsRequest{
		Namespace:   mw.namespace,
		PartitionID: mc.id,
		Inode:       inode,
	}

	packet := proto.NewPacket()
	packet.Opcode = proto.OpMetaExtentsList
	err = packet.MarshalData(req)
	if err != nil {
		log.Println(err)
		return
	}

	packet, err = mc.send(packet)
	if err != nil {
		log.Println(err)
		return
	}

	status = parseStatus(packet.ResultCode)
	if status != statusOK {
		extents = make([]proto.ExtentKey, 0)
		return
	}

	resp := new(proto.GetExtentsResponse)
	err = packet.UnmarshalData(resp)
	if err != nil {
		return
	}
	return statusOK, resp.Extents, nil
}
