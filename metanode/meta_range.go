package metanode

import (
	"encoding/binary"
	"github.com/juju/errors"
	"github.com/tiglabs/baudstorage/raftopt"
	"github.com/tiglabs/baudstorage/util/log"
	"github.com/tiglabs/raft"
	"path"
	"regexp"
	"strconv"
	"strings"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/tiglabs/baudstorage/proto"
	"github.com/tiglabs/baudstorage/sdk/stream"
)

// Errors
var (
	ErrInodeOutOfRange = errors.New("inode ID out of range.")
)

// Regexp
var (
	regexpRange, _ = regexp.Compile("(\\d)+_(\\d)+$")
)

const (
	rangeIDPartSeparator   = "_"    // Separator for range parts.
	rangePeerPartSeparator = ","    // Separator for range peers.
	metaDataDirName        = "meta" // Directory name for meta data storage managed by RocksDB.
	raftDataDirName        = "raft" // Directory name for raft data storage managed by RocksDB.
	storeKeyRangeId        = "info_range_id"
	storeKeyRangeStart     = "info_range_start"
	storeKeyRangeEnd       = "info_range_end"
	storeKeyRangeCursor    = "info_range_cursor"
	storeKeyRangePeers     = "info_range_peers"
	storeKeyRaftId         = "info_raft_id"
	storeKeyRaftGroupId    = "info_raft_group_id"
)

// MetaRange manages necessary information of meta range, include ID, boundary of range and raft identity.
// When a new inode is requested, MetaRange allocates the inode id for this inode is possible.
// States:
//  +-----+             +-------+
//  | New | → Restore → | Ready |
//  +-----+             +-------+
type MetaRange struct {
	id          string // Consist with 'namespace_start_end'. (Required when initialize)
	start       uint64 // Start inode ID of this range. (Required when initialize)
	end         uint64 // End inode ID of this range. (Required when initialize)
	cursor      uint64 // Cursor ID value of inode what have been already assigned.
	rootDataDir string // Root data directory of this meta range. (Required when initialize)
	store       *MetaRangeFsm
	peers       []string
	raftId      uint64           // Identity for raft node.
	raftGroupId uint64           // Identity for raft group. Raft nodes in same raft group must have same group ID.
	raftServer  *raft.RaftServer // Raft server instance.
	ready       bool
}

func NewMetaRange(req *CreateMetaRangeReq) *MetaRange {
	return &MetaRange{
		id:    req.MetaId,
		start: req.Start,
		end:   req.End,
		peers: req.Members,
	}
}

// Restore range info through RocksDB.
// Note that invoker goroutine will be block until restore operation complete.
func (mr *MetaRange) Restore() {
	db := raftopt.NewRocksDBStore(path.Join(mr.rootDataDir, metaDataDirName))
	db.Open()
	defer db.Close()
	var err error
	defer func() {
		if err != nil {
			log.LogError("action[MeteRange.Restore],err:%v", err)
		}
	}()
	// Try recover and validate range ID.
	rangeIdBytes, err := db.Get(storeKeyRangeId)
	if err != nil {
		return
	}
	if len(rangeIdBytes) != 0 {
		if string(rangeIdBytes) != mr.id {
			err = errors.New("broken data")
			return
		}
		mr.id = string(rangeIdBytes)
	}
	// Try recover range start value.
	rangeStartBytes, err := db.Get(storeKeyRangeStart)
	if err != nil {
		return
	}
	if len(rangeStartBytes) != 0 {
		mr.start = binary.BigEndian.Uint64(rangeStartBytes)
	}
	// Try recover range end value.
	rangeEndBytes, err := db.Get(storeKeyRangeEnd)
	if err != nil {
		return
	}
	if len(rangeEndBytes) != 0 {
		mr.end = binary.BigEndian.Uint64(rangeEndBytes)
	}
	// Try recover range cursor.
	rangeCursorBytes, err := db.Get(storeKeyRangeCursor)
	if err != nil {
		return
	}
	if len(rangeCursorBytes) != 0 {
		mr.cursor = binary.BigEndian.Uint64(rangeCursorBytes)
	}
	// Try recover range peers
	rangePeersBytes, err := db.Get(storeKeyRangePeers)
	if err != nil {
		return
	}
	if len(rangePeersBytes) != 0 {
		mr.peers = strings.Split(string(rangePeersBytes), rangePeerPartSeparator)
	}
	// Try recover rage ID
	raftIdBytes, err := db.Get(storeKeyRaftId)
	if err != nil {
		return
	}
	if len(raftIdBytes) != 0 {
		mr.raftId = binary.BigEndian.Uint64(raftIdBytes)
	}
	// Try recover raft group ID
	raftGroupIdBytes, err := db.Get(storeKeyRaftGroupId)
	if err != nil {
		return
	}
	if len(raftGroupIdBytes) != 0 {
		mr.raftGroupId = binary.BigEndian.Uint64(raftGroupIdBytes)
	}
	// Init and recover FSM.
	fsmConfig := MetaRangeFsmConfig{
		RaftId:      mr.raftId,
		RaftGroupId: mr.raftGroupId,
		Raft:        mr.raftServer,
		MetaDataDir: path.Join(mr.rootDataDir, metaDataDirName),
		RaftDataDir: path.Join(mr.rootDataDir, raftDataDirName),
	}
	mr.store = NewMetaRangeFsm(fsmConfig)
	mr.store.RegisterCursorUpdateHandler(func(inodeId uint64) {
		if inodeId > atomic.LoadUint64(&mr.cursor) {
			atomic.StoreUint64(&mr.cursor, inodeId)
		}
	})
	go mr.store.Restore()
	ready := true
	atomic.StorePointer((*unsafe.Pointer)(unsafe.Pointer(&mr.ready)), unsafe.Pointer(&ready))
}

// UpdatePeers
func (mr *MetaRange) UpdatePeers(peers []string) {
	mr.peers = peers
}

// NextInodeId returns a new ID value of inode and update offset.
// If inode ID is out of this MetaRange limit then return ErrInodeOutOfRange error.
func (mr *MetaRange) nextInodeID() (inodeId uint64, err error) {
	for {
		cur := mr.cursor
		end := mr.end
		if cur >= end {
			return 0, ErrInodeOutOfRange
		}
		newId := cur + 1
		if atomic.CompareAndSwapUint64(&mr.cursor, cur, newId) {
			return newId, nil
		}
	}
}

func (mr *MetaRange) CreateDentry(req *CreateDentryReq) (resp *CreateDentryResp) {
	dentry := &Dentry{
		ParentId: req.ParentID,
		Name:     req.Name,
		Inode:    req.Inode,
		Type:     req.Mode,
	}
	status := mr.store.CreateDentry(dentry)
	resp.Status = status
	return
}

func (mr *MetaRange) DeleteDentry(req *DeleteDentryReq) (resp *DeleteDentryResp) {
	dentry := &Dentry{
		ParentId: req.ParentID,
		Name:     req.Name,
	}
	status := mr.store.DeleteDentry(dentry)
	resp.Status = status
	resp.Inode = dentry.Inode
	return
}

func (mr *MetaRange) CreateInode(req *CreateInoReq) (resp *CreateInoResp) {
	var err error
	resp.Inode, err = mr.nextInodeID()
	if err != nil {
		resp.Status = proto.OpInodeFullErr
		return
	}
	ts := time.Now().Unix()
	ino := &Inode{
		Inode:      resp.Inode,
		Type:       req.Mode,
		AccessTime: ts,
		ModifyTime: ts,
		Stream:     stream.NewStreamKey(resp.Inode),
	}
	resp.Status = mr.store.CreateInode(ino)
	return
}

func (mr *MetaRange) DeleteInode(req *deleteInoReq) (resp *deleteInoResp) {
	ino := &Inode{
		Inode: req.Inode,
	}
	resp.Status = mr.store.DeleteInode(ino)
	return
}

func (mr *MetaRange) PutStreamKey() {
	return
}

func (mr *MetaRange) ReadDir(req *ReadDirReq) (resp *ReadDirResp) {
	// TODO: Implement read dir operation.
	resp = mr.store.ReadDir(req)
	return
}

func (mr *MetaRange) Open(req *OpenReq) (resp *OpenResp) {
	// TODO: Implement open operation.
	resp = mr.store.OpenFile(req)
	return
}
