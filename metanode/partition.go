package metanode

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/tiglabs/baudstorage/util/log"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/btree"
	"github.com/tiglabs/baudstorage/proto"
	"github.com/tiglabs/baudstorage/raftstore"
	raftproto "github.com/tiglabs/raft/proto"
)

const (
	defaultBTreeDegree = 32
)

const (
	defaultDumpRate = time.Second * 30
)

// Errors
var (
	ErrInodeOutOfRange = errors.New("inode ID out of range")
)

// MetRangeConfig used by create metaPartition and serialize
type MetaPartitionConfig struct {
	ID          string       `json:"id"`            // Consist with. (Required when initialize)
	Start       uint64       `json:"start"`         // Minimal Inode ID of this range. (Required when initialize)
	End         uint64       `json:"end"`           // Maximal Inode ID of this range. (Required when initialize)
	Cursor      uint64       `json:"cursor"`        // Cursor ID value of Inode what have been already assigned.
	Peers       []proto.Peer `json:"peers"`         // Peers information for raft.
	RaftGroupId uint64       `json:"raft_group_id"` // Identity for raft group.Raft nodes in same raft group must have same groupID.
	DataPath    string       `json:"_"`
}

func (c *MetaPartitionConfig) Dump() ([]byte, error) {
	return json.Marshal(c)
}

func (c *MetaPartitionConfig) Load(bytes []byte) error {
	return json.Unmarshal(bytes, c)
}

type OpInode interface {
	CreateInode(req *CreateInoReq, p *Packet) (err error)
	DeleteInode(req *DeleteInoReq, p *Packet) (err error)
	InodeGet(req *proto.InodeGetRequest, p *Packet) (err error)
	Open(req *OpenReq, p *Packet) (err error)
}

type OpDentry interface {
	CreateDentry(req *CreateDentryReq, p *Packet) (err error)
	DeleteDentry(req *DeleteDentryReq, p *Packet) (err error)
	ReadDir(req *ReadDirReq, p *Packet) (err error)
	Lookup(req *LookupReq, p *Packet) (err error)
}

type OpExtent interface {
	ExtentAppend(req *proto.AppendExtentKeyRequest, p *Packet) (err error)
	ExtentsList(req *proto.GetExtentsRequest, p *Packet) (err error)
}

type OpMeta interface {
	OpInode
	OpDentry
	OpExtent
}

type MetaPartition interface {
	Start() error
	Stop()
	IsLeader() (leaderAddr string, isLeader bool)
	OpMeta
}

// metaPartition manages necessary information of meta range, include ID, boundary of range and raft identity.
// When a new Inode is requested, metaPartition allocates the Inode id for this Inode is possible.
// States:
//  +-----+             +-------+
//  | New | → Restore → | Ready |
//  +-----+             +-------+
type metaPartition struct {
	config        *MetaPartitionConfig
	leaderID      uint64
	applyID       uint64       // For store Inode/Dentry max applyID, this index will be update after restore from dump data.
	dentryMu      sync.RWMutex // Mutex for Dentry operation.
	dentryTree    *btree.BTree // B-Tree for Dentry.
	inodeMu       sync.RWMutex // Mutex for Inode operation.
	inodeTree     *btree.BTree // B-Tree for Inode.
	opCounter     uint64       // Counter for meta operation, this index will be reset after data dumped into storage.
	raftGroupId   uint64
	raftStore     raftstore.RaftStore
	raftPartition raftstore.Partition // Raft partition instance of this meta partition.
	scheduleStopC chan bool
	state         ServiceState
}

func (mp *metaPartition) Start() (err error) {
	if TrySwitchState(&mp.state, stateReady, stateRunning) {
		defer func() {
			if err != nil {
				SetState(&mp.state, stateReady)
			}
		}()
		err = mp.onStart()
	}
	return
}

func (mp *metaPartition) Stop() {
	if TrySwitchState(&mp.state, stateRunning, stateReady) {
		mp.onStop()
	}
}

func (mp *metaPartition) onStart() (err error) {
	if err = mp.load(); err != nil {
		return
	}
	mp.startSchedule()
	if err = mp.startRaft(); err != nil {
		return
	}
	return
}

func (mp *metaPartition) onStop() {
	mp.stopRaft()
	mp.stopSchedule()
	mp.store()
}

func (mp *metaPartition) startSchedule() {
	mp.scheduleStopC = make(chan bool)
	go func(stopC chan bool) {
		timer := time.NewTicker(defaultDumpRate)
		for {
			select {
			case <-stopC:
				timer.Stop()
				return
			case <-timer.C:
				if err := mp.store(); err != nil {
					log.LogError(fmt.Sprintf("dump meta partition %s fail cause %s", mp.config.ID, err))
				}
			}
		}
	}(mp.scheduleStopC)
}

func (mp *metaPartition) stopSchedule() {
	if mp.scheduleStopC != nil {
		close(mp.scheduleStopC)
	}
}

func (mp *metaPartition) startRaft() (err error) {
	peers := make([]raftproto.Peer, len(mp.config.Peers))
	for _, peer := range mp.config.Peers {
		raftPeer := raftproto.Peer{
			ID: peer.ID,
		}
		peers = append(peers, raftPeer)
		mp.raftPartition.AddNode(peer.ID, peer.Addr)
	}
	pc := &raftstore.PartitionConfig{
		ID:      mp.raftGroupId,
		Applied: mp.applyID,
		Peers:   peers,
		SM:      mp,
	}
	mp.raftPartition, err = mp.raftStore.CreatePartition(pc)
	return
}

func (mp *metaPartition) stopRaft() {
	mp.raftPartition.Stop()
	return
}

// NewMetaPartition create and init a new meta partition with specified configuration.
func NewMetaPartition(conf *MetaPartitionConfig, raft raftstore.RaftStore) MetaPartition {
	mp := &metaPartition{
		config:     conf,
		dentryTree: btree.New(defaultBTreeDegree),
		inodeTree:  btree.New(defaultBTreeDegree),
		raftStore:  raft,
	}
	return mp
}

func (mp *metaPartition) IsLeader() (leaderAddr string, ok bool) {
	ok = mp.raftPartition.IsLeader()
	leaderID := mp.leaderID
	if leaderID == 0 {
		return
	}
	for _, peer := range mp.config.Peers {
		if leaderID == peer.ID {
			leaderAddr = peer.Addr
			return
		}
	}

	return
}

// Load used when metaNode start and recover data from snapshot
func (mp *metaPartition) load() (err error) {
	if err = mp.loadMeta(); err != nil {
		return
	}
	if err = mp.loadInode(); err != nil {
		return
	}
	if err = mp.loadDentry(); err != nil {
		return
	}
	err = mp.loadApplyID()
	return
}

func (mp *metaPartition) store() (err error) {
	if err = mp.storeApplyID(); err != nil {
		return
	}
	if err = mp.storeInode(); err != nil {
		return
	}
	if err = mp.storeDentry(); err != nil {
		return
	}
	err = mp.storeMeta()
	return
}

// UpdatePeers
func (mp *metaPartition) UpdatePeers(peers []proto.Peer) {
	mp.config.Peers = peers
}

// NextInodeId returns a new ID value of Inode and update offset.
// If Inode ID is out of this metaPartition limit then return ErrInodeOutOfRange error.
func (mp *metaPartition) nextInodeID() (inodeId uint64, err error) {
	for {
		cur := mp.config.Cursor
		end := mp.config.End
		if cur >= end {
			return 0, ErrInodeOutOfRange
		}
		newId := cur + 1
		if atomic.CompareAndSwapUint64(&mp.config.Cursor, cur, newId) {
			return newId, nil
		}
	}
}
