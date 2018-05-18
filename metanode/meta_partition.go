package metanode

import (
	"encoding/json"
	"errors"
	"io/ioutil"
	"os"
	"path"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/btree"
	"github.com/tiglabs/baudstorage/proto"
	"github.com/tiglabs/baudstorage/raftstore"
)

const (
	defaultBTreeDegree = 32
)

// Errors
var (
	ErrInodeOutOfRange = errors.New("inode ID out of range.")
)

/* MetRangeConfig used by create metaPartition and serialize
*  ID:		Consist with. (Required when initialize)
*  Start:	Start inode ID of this range. (Required when initialize)
*  End:		End inode ID of this range. (Required when initialize)
*  Cursor:	Cursor ID value of inode what have been already assigned.
*  RaftGroupID:	Identity for raft group.Raft nodes in same raft group must have same groupID.
*  LeaderID:	-1: non leader; >0: leader ID
*  RaftPartition:	Raft instance.
 */
type MetaPartitionConfig struct {
	ID            string              `json:"id"`
	Start         uint64              `json:"start"`
	End           uint64              `json:"end"`
	Cursor        uint64              `json:"-"`
	RootDir       string              `json:"-"`
	Peers         []proto.Peer        `json:"peers"`
	RaftGroupID   uint64              `json:"raftGroupID"`
	LeaderID      uint64              `json:"-"`
	RaftPartition raftstore.Partition `json:"-"`
	MetaManager   *MetaManager        `json:"-"`
}

// MetaPartition manages necessary information of meta range, include ID, boundary of range and raft identity.
// When a new inode is requested, MetaPartition allocates the inode id for this inode is possible.
// States:
//  +-----+             +-------+
//  | New | → Restore → | Ready |
//  +-----+             +-------+
type MetaPartition struct {
	MetaPartitionConfig
	stopC      chan struct{}
	applyID    uint64       // for store inode/dentry max applyID
	dentryMu   sync.RWMutex // Mutex for dentry operation.
	dentryTree *btree.BTree // B-Tree for dentry.
	inodeMu    sync.RWMutex // Mutex for inode operation.
	inodeTree  *btree.BTree // B-Tree for inode.
}

func NewMetaPartition(conf MetaPartitionConfig) *MetaPartition {
	mp := &MetaPartition{
		MetaPartitionConfig: conf,
		stopC:               make(chan struct{}, 1),
		dentryTree:          btree.New(defaultBTreeDegree),
		inodeTree:           btree.New(defaultBTreeDegree),
	}
	return mp
}

func (mp *MetaPartition) isLeader() (leaderAddr string, ok bool) {
	ok = mp.RaftPartition.IsLeader()
	leaderID := mp.LeaderID
	if leaderID == 0 {
		return
	}
	for _, peer := range mp.Peers {
		if leaderID == peer.ID {
			leaderAddr = peer.Addr
			return
		}
	}

	return
}

func (mp *MetaPartition) Sizeof() uint64 {

	return 0
}

// Load used when metaNode start and recover data from snapshot
func (mp *MetaPartition) Load() (err error) {
	if err = mp.LoadMeta(); err != nil {
		return
	}
	if err = mp.LoadInode(); err != nil {
		return
	}
	if err = mp.LoadDentry(); err != nil {
		return
	}
	// Restore ApplyID
	if err = mp.LoadApplyID(); err != nil {
		return
	}
	return
}

// Load range meta from meta snapshot file.
func (mp *MetaPartition) LoadMeta() (err error) {
	// Load struct from meta
	metaFile := path.Join(mp.RootDir, "meta")
	fp, err := os.OpenFile(metaFile, os.O_RDONLY, 0655)
	if err != nil {
		return
	}
	defer fp.Close()
	data, err := ioutil.ReadAll(fp)
	if err != nil || len(data) == 0 {
		return
	}
	var mConf MetaPartitionConfig
	if err = json.Unmarshal(data, &mConf); err != nil {
		return
	}
	mp.MetaPartitionConfig = mConf
	return
}

func (mp *MetaPartition) StoreMeta() (err error) {
	// Store Meta to file
	metaFile := path.Join(mp.RootDir, ".meta")
	fp, err := os.OpenFile(metaFile, os.O_RDWR|os.O_TRUNC|os.O_APPEND|os.O_CREATE,
		0655)
	if err != nil {
		return
	}
	defer func() {
		fp.Close()
		if err != nil {
			os.Remove(metaFile)
		}
	}()
	data, err := json.Marshal(mp)
	if err != nil {
		return
	}
	if _, err = fp.Write(data); err != nil {
		return
	}
	err = os.Rename(metaFile, path.Join(mp.RootDir, "meta"))
	return
}

func (mp *MetaPartition) StartStoreSchedule() {
	t := time.NewTicker(5 * time.Minute)
	next := time.Now().Add(time.Hour)
	curApplyID := mp.applyID
	for {
		select {
		case <-mp.stopC:
			return
		case <-t.C:
			now := time.Now()
			if now.After(next) {
				next = now.Add(time.Hour)
				if (mp.applyID - curApplyID) > 0 {
					curApplyID = mp.applyID
					goto store
				}
				goto end
			} else if (mp.applyID - curApplyID) > 20000 {
				next = now.Add(time.Hour)
				curApplyID = mp.applyID
				goto store
			}
			goto end
		}
	store:
		// 1st: load applyID
		if err := mp.StoreApplyID(); err != nil {
			//TODO: Log
			goto end
		}
		// 2st: load ino tree
		if err := mp.StoreInodeTree(); err != nil {
			//TODO: Log
			goto end
		}
		// 3st: load dentry tree
		if err := mp.StoreDentryTree(); err != nil {
			//TODO: Log
			goto end

		}
		// rename
		if err := os.Rename(path.Join(mp.RootDir, "_inode"), path.Join(mp.RootDir, "inode")); err != nil {
			//TODO: Log
			goto end
		}
		if err := os.Rename(path.Join(mp.RootDir, "_dentry"), path.Join(mp.RootDir, "dentry")); err != nil {
			//TODO: Log
			goto end
		}
		if err := os.Rename(path.Join(mp.RootDir, "_applyid"), path.Join(mp.RootDir, "applyid")); err != nil {
			//TODO: Log
			goto end
		}
	end:
		os.Remove(path.Join(mp.RootDir, "_applyid"))
		os.Remove(path.Join(mp.RootDir, "_inode"))
		os.Remove(path.Join(mp.RootDir, "_dentry"))
	}
	return
}

func (mp *MetaPartition) Stop() {
	mp.stopC <- struct{}{}
	mp.RaftPartition.Stop()
}

// UpdatePeers
func (mp *MetaPartition) UpdatePeers(peers []proto.Peer) {
	mp.Peers = peers
}

// NextInodeId returns a new ID value of inode and update offset.
// If inode ID is out of this MetaPartition limit then return ErrInodeOutOfRange error.
func (mp *MetaPartition) nextInodeID() (inodeId uint64, err error) {
	for {
		cur := mp.Cursor
		end := mp.End
		if cur >= end {
			return 0, ErrInodeOutOfRange
		}
		newId := cur + 1
		if atomic.CompareAndSwapUint64(&mp.Cursor, cur, newId) {
			return newId, nil
		}
	}
}

func (mp *MetaPartition) CreateDentry(req *CreateDentryReq, p *Packet) (err error) {
	dentry := &Dentry{
		ParentId: req.ParentID,
		Name:     req.Name,
		Inode:    req.Inode,
		Type:     req.Mode,
	}
	val, err := json.Marshal(dentry)
	if err != nil {
		p.PackErrorWithBody(proto.OpErr, nil)
		return
	}
	resp, err := mp.Put(opCreateDentry, val)
	if err != nil {
		p.PackErrorWithBody(proto.OpErr, nil)
		return
	}
	p.PackErrorWithBody(resp.(uint8), nil)
	return
}

func (mp *MetaPartition) DeleteDentry(req *DeleteDentryReq, p *Packet) (err error) {
	var resp *DeleteDentryResp
	dentry := &Dentry{
		ParentId: req.ParentID,
		Name:     req.Name,
	}
	val, err := json.Marshal(dentry)
	if err != nil {
		p.ResultCode = proto.OpErr
		return
	}
	r, err := mp.Put(opDeleteDentry, val)
	if err != nil {
		p.ResultCode = proto.OpErr
		return
	}
	p.ResultCode = r.(uint8)
	if p.ResultCode == proto.OpOk {
		var reply []byte
		resp.Inode = dentry.Inode
		reply, err = json.Marshal(resp)
		p.PackOkWithBody(reply)
	}
	return
}

func (mp *MetaPartition) CreateInode(req *CreateInoReq, p *Packet) (err error) {
	inoID, err := mp.nextInodeID()
	if err != nil {
		err = nil
		p.PackErrorWithBody(proto.OpInodeFullErr, nil)
		return
	}
	ino := NewInode(inoID, req.Mode)
	val, err := json.Marshal(ino)
	if err != nil {
		p.PackErrorWithBody(proto.OpErr, nil)
		return
	}
	r, err := mp.Put(opCreateInode, val)
	if err != nil {
		p.PackErrorWithBody(proto.OpErr, nil)
		return
	}
	var (
		status = r.(uint8)
		reply  []byte
	)
	if status == proto.OpOk {
		resp := &CreateInoResp{}
		resp.Info.Inode = ino.Inode
		resp.Info.Mode = ino.Type
		resp.Info.Size = ino.Size
		resp.Info.CreateTime = time.Unix(ino.CreateTime, 0)
		resp.Info.ModifyTime = time.Unix(ino.ModifyTime, 0)
		resp.Info.AccessTime = time.Unix(ino.AccessTime, 0)
		reply, err = json.Marshal(resp)
		if err != nil {
			status = proto.OpErr
		}
	}
	p.PackErrorWithBody(status, reply)
	return
}

func (mp *MetaPartition) DeleteInode(req *DeleteInoReq, p *Packet) (err error) {
	ino := NewInode(req.Inode, 0)
	val, err := json.Marshal(ino)
	if err != nil {
		p.PackErrorWithBody(proto.OpErr, nil)
		return
	}
	r, err := mp.Put(opDeleteInode, val)
	if err != nil {
		p.PackErrorWithBody(proto.OpErr, nil)
		return
	}
	p.PackErrorWithBody(r.(uint8), nil)
	return
}

func (mp *MetaPartition) ReadDir(req *ReadDirReq, p *Packet) (err error) {
	// TODO: Implement read dir operation.
	resp := mp.readDir(req)
	reply, err := json.Marshal(resp)
	if err != nil {
		p.PackErrorWithBody(proto.OpErr, nil)
		return
	}
	p.PackOkWithBody(reply)
	return
}

func (mp *MetaPartition) Open(req *OpenReq, p *Packet) (err error) {
	// TODO: Implement open operation.
	ino := NewInode(req.Inode, 0)
	val, err := json.Marshal(ino)
	if err != nil {
		p.PackErrorWithBody(proto.OpErr, nil)
		return
	}
	resp, err := mp.Put(opOpen, val)
	p.PackErrorWithBody(resp.(uint8), nil)
	return
}

func (mp *MetaPartition) InodeGet(req *proto.InodeGetRequest,
	p *Packet) (err error) {
	ino := NewInode(req.Inode, 0)
	if err != nil {
		p.PackErrorWithBody(proto.OpErr, nil)
		return
	}
	status := mp.getInode(ino)
	var reply []byte
	if status == proto.OpOk {
		resp := &proto.InodeGetResponse{
			Info: &proto.InodeInfo{},
		}
		resp.Info.Inode = ino.Inode
		resp.Info.Mode = ino.Type
		resp.Info.Size = ino.Size
		resp.Info.CreateTime = time.Unix(ino.CreateTime, 0)
		resp.Info.AccessTime = time.Unix(ino.AccessTime, 0)
		resp.Info.ModifyTime = time.Unix(ino.ModifyTime, 0)
		reply, err = json.Marshal(resp)
		if err != nil {
			status = proto.OpErr
		}
	}
	p.PackErrorWithBody(status, reply)
	return
}

func (mp *MetaPartition) ExtentAppend(req *proto.AppendExtentKeyRequest, p *Packet) (err error) {
	ino := NewInode(req.Inode, 0)
	ino.Extents = req.Extents
	val, err := json.Marshal(ino)
	if err != nil {
		p.PackErrorWithBody(proto.OpErr, nil)
		return
	}
	resp, err := mp.Put(opExtentsAdd, val)
	if err != nil {
		p.PackErrorWithBody(proto.OpErr, nil)
		return
	}
	p.PackErrorWithBody(resp.(uint8), nil)
	return
}

func (mp *MetaPartition) ExtentsList(req *proto.GetExtentsRequest,
	p *Packet) (err error) {
	ino := NewInode(req.Inode, 0)
	status := mp.getInode(ino)
	var reply []byte
	if status == proto.OpOk {
		resp := &proto.GetExtentsResponse{
			Extents: ino.Extents,
		}
		reply, err = json.Marshal(resp)
		if err != nil {
			status = proto.OpErr
		}
	}
	p.PackErrorWithBody(status, reply)
	return
}

func (mp *MetaPartition) DeletePartition() (err error) {
	_, err = mp.Put(opDeletePartition, nil)
	return
}

func (mp *MetaPartition) UpdatePartition(req *proto.UpdateMetaPartitionRequest) (err error) {
	reqData, err := json.Marshal(req)
	if err != nil {
		return
	}
	_, err = mp.Put(opUpdatePartition, reqData)
	return
}

func (mp *MetaPartition) OfflienPartition(req []byte) (err error) {
	_, err = mp.Put(opOfflinePartition, req)
	return
}
