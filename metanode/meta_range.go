package metanode

import (
	"encoding/json"
	"errors"
	"io/ioutil"
	"os"
	"path"
	"sync/atomic"
	"time"

	"github.com/tiglabs/baudstorage/proto"
	"github.com/tiglabs/baudstorage/sdk/stream"
	"github.com/tiglabs/raft"
)

// Errors
var (
	ErrInodeOutOfRange = errors.New("inode ID out of range.")
)

/* MetRangeConfig used by create metaRange and serialize
*  id:		Consist with 'namespace_ID'. (Required when initialize)
*  start:	Start inode ID of this range. (Required when initialize)
*  end:		End inode ID of this range. (Required when initialize)
*  cursor:	Cursor ID value of inode what have been already assigned.
*  raftGroupID:	Identity for raft group.Raft nodes in same raft group must have same groupID.
*  raftServer:	Raft server instance.
 */
type MetaRangeConfig struct {
	ID          string           `json:"id"`
	Start       uint64           `json:"start"`
	End         uint64           `json:"end"`
	Cursor      uint64           `json:"-"`
	RootDir     string           `json:"-"`
	Peers       []string         `json:"peers"`
	RaftGroupID uint64           `json:"raftGroupID"`
	RaftServer  *raft.RaftServer `json:"-"`
}

// MetaRange manages necessary information of meta range, include ID, boundary of range and raft identity.
// When a new inode is requested, MetaRange allocates the inode id for this inode is possible.
// States:
//  +-----+             +-------+
//  | New | → Restore → | Ready |
//  +-----+             +-------+
type MetaRange struct {
	MetaRangeConfig
	store *MetaRangeFsm
}

func NewMetaRange(conf MetaRangeConfig) *MetaRange {
	mr := &MetaRange{
		MetaRangeConfig: conf,
	}
	mr.store = NewMetaRangeFsm(mr)
	return mr
}

// Load used when metaNode start and recover data from snapshot
func (mr *MetaRange) Load() (err error) {
	if err = mr.LoadMeta(); err != nil {
		return
	}
	if err = mr.store.LoadInode(); err != nil {
		return
	}
	if err = mr.store.LoadDentry(); err != nil {
		return
	}
	// Restore ApplyID
	if err = mr.store.LoadApplyID(); err != nil {
		return
	}
	return
}

// Load range meta from meta snapshot file.
func (mr *MetaRange) LoadMeta() (err error) {
	// Restore struct from meta
	metaFile := path.Join(mr.RootDir, "meta")
	fp, err := os.OpenFile(metaFile, os.O_RDONLY, 0655)
	if err != nil {
		return
	}
	defer fp.Close()
	data, err := ioutil.ReadAll(fp)
	if err != nil || len(data) == 0 {
		return
	}
	var mConf MetaRangeConfig
	if err = json.Unmarshal(data, &mConf); err != nil {
		return
	}
	mr.MetaRangeConfig = mConf
	return
}

func (mr *MetaRange) StartStoreSchedule() {
	t := time.NewTicker(5 * time.Minute)
	next := time.Now().Add(time.Hour)
	curApplyID := mr.store.applyID
	for {
		select {
		case <-t.C:
			now := time.Now()
			if now.After(next) {
				next = now.Add(time.Hour)
				if (mr.store.applyID - curApplyID) > 0 {
					curApplyID = mr.store.applyID
					goto store
				}
				goto end
			} else if (mr.store.applyID - curApplyID) > 20000 {
				next = now.Add(time.Hour)
				curApplyID = mr.store.applyID
				goto store
			}
			goto end
		}
	store:
		// 1st: load applyID
		if err := mr.store.StoreApplyID(); err != nil {
			//TODO: Log
			goto end
		}
		// 2st: load ino tree
		if err := mr.store.StoreInodeTree(); err != nil {
			//TODO: Log
			goto end
		}
		// 3st: load dentry tree
		if err := mr.store.StoreDentryTree(); err != nil {
			//TODO: Log
			goto end

		}
		// rename
		if err := os.Rename(path.Join(mr.RootDir, "_inode"), path.Join(mr.RootDir, "inode")); err != nil {
			//TODO: Log
			goto end
		}
		if err := os.Rename(path.Join(mr.RootDir, "_dentry"), path.Join(mr.RootDir, "dentry")); err != nil {
			//TODO: Log
			goto end
		}
		if err := os.Rename(path.Join(mr.RootDir, "_applyid"), path.Join(mr.RootDir, "applyid")); err != nil {
			//TODO: Log
			goto end
		}
	end:
		os.Remove(path.Join(mr.RootDir, "_applyid"))
		os.Remove(path.Join(mr.RootDir, "_inode"))
		os.Remove(path.Join(mr.RootDir, "_dentry"))
	}
	return
}

// UpdatePeers
func (mr *MetaRange) UpdatePeers(peers []string) {
	mr.Peers = peers
}

// NextInodeId returns a new ID value of inode and update offset.
// If inode ID is out of this MetaRange limit then return ErrInodeOutOfRange error.
func (mr *MetaRange) nextInodeID() (inodeId uint64, err error) {
	for {
		cur := mr.Cursor
		end := mr.End
		if cur >= end {
			return 0, ErrInodeOutOfRange
		}
		newId := cur + 1
		if atomic.CompareAndSwapUint64(&mr.Cursor, cur, newId) {
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
	resp.Info.Inode, err = mr.nextInodeID()
	if err != nil {
		resp.Status = proto.OpInodeFullErr
		return
	}
	ts := time.Now().Unix()
	ino := &Inode{
		Inode:      resp.Info.Inode,
		Type:       req.Mode,
		AccessTime: ts,
		ModifyTime: ts,
		Stream:     stream.NewStreamKey(resp.Info.Inode),
	}
	resp.Status = mr.store.CreateInode(ino)
	return
}

func (mr *MetaRange) DeleteInode(req *DeleteInoReq) (resp *DeleteInoResp) {
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
