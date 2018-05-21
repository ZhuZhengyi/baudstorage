package metanode

import (
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/btree"
	"github.com/tiglabs/baudstorage/proto"
	"github.com/tiglabs/baudstorage/raftstore"
	"github.com/tiglabs/baudstorage/util/log"
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

/* MetRangeConfig used by create metaPartition and serialize
PartitionId: Identity for raftStore group,RaftStore nodes in same raftStore group must have same groupID.
Start: Minimal Inode ID of this range. (Required when initialize)
End: Maximal Inode ID of this range. (Required when initialize)
Cursor: Cursor ID value of Inode what have been already assigned.
Peers: Peers information for raftStore.
*/
type MetaPartitionConfig struct {
	PartitionId uint64              `json:"partition_id"`
	Start       uint64              `json:"start"`
	End         uint64              `json:"end"`
	Cursor      uint64              `json:"-"`
	Peers       []proto.Peer        `json:"peers"`
	NodeId      uint64              `json:"-"`
	RootDir     string              `json:"-"`
	BeforeStart func()              `json:"-"`
	AfterStart  func()              `json:"-"`
	BeforeStop  func()              `json:"-"`
	AfterStop   func()              `json:"-"`
	RaftStore   raftstore.RaftStore `json:"-"`
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
	OpConfig
}

type OpConfig interface {
	IsLeader() (leaderAddr string, isLeader bool)
	GetCursor() uint64
	GetBaseConfig() *MetaPartitionConfig
	ChangeMember(changeType raftproto.ConfChangeType, peer raftproto.Peer, context []byte) (resp interface{}, err error)
}

type MetaPartition interface {
	Start() error
	Stop()
	OpMeta
}

// metaPartition manages necessary information of meta range, include ID, boundary of range and raftStore identity.
// When a new Inode is requested, metaPartition allocates the Inode id for this Inode is possible.
// States:
//  +-----+             +-------+
//  | New | → Restore → | Ready |
//  +-----+             +-------+
type metaPartition struct {
	config     *MetaPartitionConfig
	leaderID   uint64
	applyID    uint64       // For store Inode/Dentry max applyID, this index will be update after restore from dump data.
	dentryMu   sync.RWMutex // Mutex for Dentry operation.
	dentryTree *btree.BTree // B-Tree for Dentry.
	inodeMu    sync.RWMutex // Mutex for Inode operation.
	inodeTree  *btree.BTree // B-Tree for Inode.
	opCounter  uint64       // Counter for meta operation, this index will be reset after data dumped into storage.

	raftPartition raftstore.Partition // RaftStore partition instance of this meta partition.
	stopC         chan bool
	state         ServiceState
}

func (mp *metaPartition) Start() (err error) {
	if TrySwitchState(&mp.state, stateReady, stateRunning) {
		defer func() {
			if err != nil {
				SetState(&mp.state, stateReady)
			}
		}()
		if mp.config.BeforeStart != nil {
			mp.config.BeforeStart()
		}
		err = mp.onStart()
		if mp.config.AfterStart != nil {
			mp.config.AfterStart()
		}
	}
	return
}

func (mp *metaPartition) Stop() {
	if TrySwitchState(&mp.state, stateRunning, stateReady) {
		if mp.config.BeforeStop != nil {
			mp.config.BeforeStop()
		}
		mp.onStop()
		if mp.config.AfterStop != nil {
			mp.config.AfterStop()
		}
	}
}

func (mp *metaPartition) onStart() (err error) {
	if err = mp.load(); err != nil {
		return
	}
	if err = mp.startRaft(); err != nil {
		return
	}
	mp.startSchedule()
	return
}

func (mp *metaPartition) onStop() {
	mp.stopRaft()
	mp.stopSchedule()
	mp.store()
}

func (mp *metaPartition) startSchedule() {
	mp.stopC = make(chan bool)
	go func(stopC chan bool) {
		timer := time.NewTicker(defaultDumpRate)
		for {
			select {
			case <-stopC:
				timer.Stop()
				return
			case <-timer.C:
				if err := mp.store(); err != nil {
					log.LogError(fmt.Sprintf(
						"dump meta partition %d fail cause %s", mp.config.PartitionId,
						err))
				}
			}
		}
	}(mp.stopC)
}

func (mp *metaPartition) stopSchedule() {
	if mp.stopC != nil {
		close(mp.stopC)
	}
}

func (mp *metaPartition) startRaft() (
	err error) {
	peers := make([]raftproto.Peer, len(mp.config.Peers))
	for _, peer := range mp.config.Peers {
		raftPeer := raftproto.Peer{
			ID: peer.ID,
		}
		peers = append(peers, raftPeer)
		mp.raftPartition.AddNode(peer.ID, peer.Addr)
	}
	pc := &raftstore.PartitionConfig{
		ID:      mp.config.PartitionId,
		Applied: mp.applyID,
		Peers:   peers,
		SM:      mp,
	}
	mp.raftPartition, err = mp.config.RaftStore.CreatePartition(pc)
	return
}

func (mp *metaPartition) stopRaft() {
	mp.raftPartition.Stop()
	return
}

// NewMetaPartition create and init a new meta partition with specified configuration.
func NewMetaPartition(conf *MetaPartitionConfig) MetaPartition {
	mp := &metaPartition{
		config:     conf,
		dentryTree: btree.New(defaultBTreeDegree),
		inodeTree:  btree.New(defaultBTreeDegree),
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

func (mp *metaPartition) GetCursor() uint64 {
	return mp.config.Cursor
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

func (mp *metaPartition) ChangeMember(changeType raftproto.ConfChangeType, peer raftproto.Peer, context []byte) (resp interface{}, err error) {
	resp, err = mp.raftPartition.ChangeMember(changeType, peer, context)
	return
}

func (mp *metaPartition) GetBaseConfig() *MetaPartitionConfig {
	return mp.config
}
