package metanode

import (
	"encoding/json"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/btree"
	"github.com/juju/errors"
	"github.com/tiglabs/baudstorage/proto"
	"github.com/tiglabs/baudstorage/raftstore"
	"github.com/tiglabs/baudstorage/util/log"
	"github.com/tiglabs/baudstorage/util/ump"
	raftproto "github.com/tiglabs/raft/proto"
)

const (
	defaultBTreeDegree = 32
)

const (
	storeTimeTicker = time.Minute * 5
)

var (
	ErrIllegalHeartbeatAddress = errors.New("illegal heartbeat address")
	ErrIllegalReplicateAddress = errors.New("illegal replicate address")
)

// Errors
var (
	ErrInodeOutOfRange = errors.New("inode ID out of range")
)

type sortPeers []proto.Peer

func (sp sortPeers) Len() int {
	return len(sp)
}
func (sp sortPeers) Less(i, j int) bool {
	return sp[i].ID < sp[j].ID
}

func (sp sortPeers) Swap(i, j int) {
	sp[i], sp[j] = sp[j], sp[i]
}

/* MetRangeConfig used by create metaPartition and serialize
PartitionId: Identity for raftStore group,RaftStore nodes in same raftStore group must have same groupID.
Start: Minimal Inode ID of this range. (Required when initialize)
End: Maximal Inode ID of this range. (Required when initialize)
Cursor: Cursor ID value of Inode what have been already assigned.
Peers: Peers information for raftStore.
*/
type MetaPartitionConfig struct {
	PartitionId uint64              `json:"partition_id"`
	NameSpace   string              `json:"namespace"`
	Start       uint64              `json:"start"`
	End         uint64              `json:"end"`
	Peers       []proto.Peer        `json:"peers"`
	Cursor      uint64              `json:"-"`
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

func (c *MetaPartitionConfig) checkMeta() (err error) {
	if c.PartitionId <= 0 {
		err = errors.Errorf("[checkMeta]: partition id at least 1, "+
			"now partition id is: %d", c.PartitionId)
		return
	}
	if c.Start < 0 {
		err = errors.Errorf("[checkMeta]: start at least 0")
		return
	}
	if c.End <= c.Start {
		err = errors.Errorf("[checkMeta]: end at least 'start'")
		return
	}
	if len(c.Peers) <= 0 {
		err = errors.Errorf("[checkMeta]: must have peers, now peers is 0")
		return
	}
	return
}

func (c *MetaPartitionConfig) sortPeers() {
	sp := sortPeers(c.Peers)
	sort.Sort(sp)
}

type OpInode interface {
	CreateInode(req *CreateInoReq, p *Packet) (err error)
	DeleteInode(req *DeleteInoReq, p *Packet) (err error)
	InodeGet(req *InodeGetReq, p *Packet) (err error)
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
	OpPartition
}

type OpPartition interface {
	IsLeader() (leaderAddr string, isLeader bool)
	GetCursor() uint64
	GetBaseConfig() MetaPartitionConfig
	StoreMeta() (err error)
	ChangeMember(changeType raftproto.ConfChangeType, peer raftproto.Peer, context []byte) (resp interface{}, err error)
	DeletePartition() (err error)
	UpdatePartition(req *proto.UpdateMetaPartitionRequest,
		resp *proto.UpdateMetaPartitionResponse) (err error)
	DeleteRaft() error
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
	config        *MetaPartitionConfig
	leaderID      uint64
	applyID       uint64              // For store Inode/Dentry max applyID, this index will be update after restore from dump data.
	dentryMu      sync.RWMutex        // Mutex for Dentry operation.
	dentryTree    *btree.BTree        // B-Tree for Dentry.
	inodeMu       sync.RWMutex        // Mutex for Inode operation.
	inodeTree     *btree.BTree        // B-Tree for Inode.
	raftPartition raftstore.Partition // RaftStore partition instance of this meta partition.
	stopC         chan bool
	state         uint32
}

func (mp *metaPartition) Start() (err error) {
	if atomic.CompareAndSwapUint32(&mp.state, StateStandby, StateStart) {
		defer func() {
			var newState uint32
			if err != nil {
				newState = StateStandby
			} else {
				newState = StateRunning
			}
			atomic.StoreUint32(&mp.state, newState)
		}()
		if mp.config.BeforeStart != nil {
			mp.config.BeforeStart()
		}
		if err = mp.onStart(); err != nil {
			err = errors.Errorf("[Start]->%s", err.Error())
			return
		}
		if mp.config.AfterStart != nil {
			mp.config.AfterStart()
		}
	}
	return
}

func (mp *metaPartition) Stop() {
	if atomic.CompareAndSwapUint32(&mp.state, StateRunning, StateShutdown) {
		defer atomic.StoreUint32(&mp.state, StateStopped)
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
		err = errors.Errorf("[onStart]:load partition id=%d: %s",
			mp.config.PartitionId, err.Error())
		return
	}
	if err = mp.startRaft(); err != nil {
		err = errors.Errorf("[onStart]start raft id=%d: %s",
			mp.config.PartitionId,
			err.Error())
		return
	}
	mp.startSchedule()
	return
}

func (mp *metaPartition) onStop() {
	mp.stopSchedule()
	mp.stopRaft()
}

func (mp *metaPartition) startSchedule() {
	mp.stopC = make(chan bool)
	go func(stopC chan bool) {
		timer := time.NewTicker(storeTimeTicker)
		var nowAppID uint64
		for {
			select {
			case <-stopC:
				timer.Stop()
				return
			case <-timer.C:
				if mp.applyID <= nowAppID {
					continue
				}
				if appID, err := mp.store(); err != nil {
					err = errors.Errorf(
						"[startSchedule]: dump partition id=%d: %v",
						mp.config.PartitionId, err.Error())
					log.LogErrorf(err.Error())
					ump.Alarm(UMPKey, err.Error())
					continue
				} else {
					nowAppID = appID
				}
				// Truncate raft log
				mp.raftPartition.Truncate(nowAppID)
			}
		}
	}(mp.stopC)
}

func (mp *metaPartition) stopSchedule() {
	if mp.stopC != nil {
		close(mp.stopC)
	}
}

func (mp *metaPartition) startRaft() (err error) {
	var (
		heartbeatPort int
		replicatePort int
		peers         []raftstore.PeerAddress
	)
	if heartbeatPort, replicatePort, err = mp.getRaftPort(); err != nil {
		return
	}
	for _, peer := range mp.config.Peers {
		addr := strings.Split(peer.Addr, ":")[0]
		rp := raftstore.PeerAddress{
			Peer: raftproto.Peer{
				ID: peer.ID,
			},
			Address:       addr,
			HeartbeatPort: heartbeatPort,
			ReplicatePort: replicatePort,
		}
		peers = append(peers, rp)
	}
	log.LogDebugf("start partition id=%d raft peers: %s",
		mp.config.PartitionId, peers)
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
	if mp.raftPartition != nil {
		mp.raftPartition.Stop()
	}
	return
}

func (mp *metaPartition) getRaftPort() (heartbeat, replicate int, err error) {
	raftConfig := mp.config.RaftStore.RaftConfig()
	heartbeatAddrParts := strings.Split(raftConfig.HeartbeatAddr, ":")
	replicateAddrParts := strings.Split(raftConfig.ReplicateAddr, ":")
	if len(heartbeatAddrParts) != 2 {
		err = ErrIllegalHeartbeatAddress
		return
	}
	if len(replicateAddrParts) != 2 {
		err = ErrIllegalReplicateAddress
		return
	}
	heartbeat, err = strconv.Atoi(heartbeatAddrParts[1])
	if err != nil {
		return
	}
	replicate, err = strconv.Atoi(replicateAddrParts[1])
	if err != nil {
		return
	}
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

func (mp *metaPartition) StoreMeta() (err error) {
	mp.config.sortPeers()
	err = mp.storeMeta()
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

func (mp *metaPartition) store() (applyID uint64, err error) {
	applyID = mp.applyID
	if err = mp.storeInode(); err != nil {
		return
	}
	if err = mp.storeDentry(); err != nil {
		return
	}
	if err = mp.storeApplyID(applyID); err != nil {
		return
	}
	return
}

// UpdatePeers
func (mp *metaPartition) UpdatePeers(peers []proto.Peer) {
	mp.config.Peers = peers
}

func (mp *metaPartition) DeleteRaft() (err error) {
	err = mp.raftPartition.Delete()
	return
}

// NextInodeId returns a new ID value of Inode and update offset.
// If Inode ID is out of this metaPartition limit then return ErrInodeOutOfRange error.
func (mp *metaPartition) nextInodeID() (inodeId uint64, err error) {
	for {
		cur := atomic.LoadUint64(&mp.config.Cursor)
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

func (mp *metaPartition) GetBaseConfig() MetaPartitionConfig {
	return *mp.config
}

func (mp *metaPartition) DeletePartition() (err error) {
	_, err = mp.Put(opDeletePartition, nil)
	return
}

func (mp *metaPartition) UpdatePartition(req *proto.
	UpdateMetaPartitionRequest, resp *proto.UpdateMetaPartitionResponse) (
	err error) {
	reqData, err := json.Marshal(req)
	if err != nil {
		resp.Status = proto.TaskFail
		resp.Result = err.Error()
		return
	}
	r, err := mp.Put(opUpdatePartition, reqData)
	resp.Status = r.(uint8)
	if err != nil {
		resp.Result = err.Error()
	}
	return
}

func (mp *metaPartition) OfflinePartition(req []byte) (err error) {
	_, err = mp.Put(opOfflinePartition, req)
	return
}
