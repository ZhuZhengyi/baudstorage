package metanode

import (
	"bufio"
	"encoding/json"
	"errors"
	"io"
	"io/ioutil"
	"os"
	"path"
	"sync/atomic"
	"time"

	"github.com/tiglabs/baudstorage/proto"
	"github.com/tiglabs/baudstorage/sdk/stream"
	"github.com/tiglabs/raft"
	raftproto "github.com/tiglabs/raft/proto"
)

// Errors
var (
	ErrInodeOutOfRange = errors.New("inode ID out of range.")
)

type MetaRangeConfig struct {
	ID          string `json:"id"`    // Consist with 'namespace_start_end'. (Required when initialize)
	Start       uint64 `json:"start"` // Start inode ID of this range. (Required when initialize)
	End         uint64 `json:"end"`   // End inode ID of this range. (Required when initialize)
	cursor      uint64 // Cursor ID value of inode what have been already assigned.
	rootDir     string
	Peers       []string         `json:"peers"`
	RaftGroupId uint64           `json:"raftGroupID"` // Identity for raft group. Raft nodes in same raft group must have same group ID.
	raftServer  *raft.RaftServer // Raft server instance.
	isRestore   bool
}

// MetaRange manages necessary information of meta range, include ID, boundary of range and raft identity.
// When a new inode is requested, MetaRange allocates the inode id for this inode is possible.
// States:
//  +-----+             +-------+
//  | New | → Restore → | Ready |
//  +-----+             +-------+
type MetaRange struct {
	MetaRangeConfig
	store          *MetaRangeFsm
	restoreApplyID uint64 // for restore inode/dentry max applyID
}

func NewMetaRange(conf MetaRangeConfig) *MetaRange {
	mr := &MetaRange{
		MetaRangeConfig: conf,
	}
	mr.store = NewMetaRangeFsm()
	return mr
}

// Restore range meta from meta snapshot file.
func (mr *MetaRange) RestoreMeta() (err error) {
	// Restore struct from meta
	metaFile := path.Join(mr.rootDir, "meta")
	fp, err := os.OpenFile(metaFile, os.O_RDONLY, 0655)
	if err != nil {
		return
	}
	defer fp.Close()
	data, err := ioutil.ReadAll(fp)
	if err != nil || len(data) == 0 {
		return
	}
	if err = json.Unmarshal(data, &mr); err != nil {
		return
	}
	//TODO:  Check Valid

	return
}

// Restore range inode from inode snapshot file
func (mf *MetaRange) RestoreInode() (err error) {
	// Restore btree from ino file
	inoFile := path.Join(mf.rootDir, "inode")
	fp, err := os.OpenFile(inoFile, os.O_RDONLY, 0644)
	if err != nil {
		return
	}
	defer fp.Close()
	reader := bufio.NewReader(fp)
	for {
		var (
			line []byte
			ino  = &Inode{}
		)
		line, _, err = reader.ReadLine()
		if err != nil {
			if err == io.EOF {
				err = nil
				return
			}
			return
		}
		//TODO: ignore error
		if err = json.Unmarshal(line, ino); err != nil {
			continue
		}
		//TODO: check valid

		if mf.store.CreateInode(ino) == proto.OpOk {
			if mf.restoreApplyID < ino.ApplyID {
				mf.restoreApplyID = ino.ApplyID
			}
		}
	}
	return
}

// Restore range dentry from dentry snapshot file
func (mf *MetaRange) RestoreDentry() (err error) {
	// Restore dentry from dentry file
	dentryFile := path.Join(mf.rootDir, "dentry")
	fp, err := os.OpenFile(dentryFile, os.O_RDONLY, 0644)
	if err != nil {
		return
	}
	defer fp.Close()
	reader := bufio.NewReader(fp)
	for {
		var (
			line   []byte
			dentry = &Dentry{}
		)
		line, _, err = reader.ReadLine()
		if err != nil {
			if err == io.EOF {
				err = nil
				return
			}
			return
		}
		//TODO: ignore error
		if err = json.Unmarshal(line, dentry); err != nil {
			continue
		}
		// TODO: check valid

		mf.store.CreateDentry(dentry)
	}
	return
}

// UpdatePeers
func (mr *MetaRange) UpdatePeers(peers []string) {
	mr.Peers = peers
}

func (mr *MetaRange) RestoreApplied() {
	// Restore from applyID to current now

	item := mr.store.GetInodeTree().Max()
	if item != nil {
		ino := item.(*Inode)
		mr.cursor = ino.Inode
	}
	mr.isRestore = false
}

// NextInodeId returns a new ID value of inode and update offset.
// If inode ID is out of this MetaRange limit then return ErrInodeOutOfRange error.
func (mr *MetaRange) nextInodeID() (inodeId uint64, err error) {
	for {
		cur := mr.cursor
		end := mr.End
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

// Implement raft StateMachine interface
func (mf *MetaRange) Apply(command []byte, index uint64) (interface{}, error) {
	return nil, nil
}

func (mf *MetaRange) ApplyMemeberChange(confChange *raftproto.ConfChange, index uint64) (interface{}, error) {
	return nil, nil
}

func (mf *MetaRange) Snapshot() (raftproto.Snapshot, error) {
	return nil, nil
}

func (mf *MetaRange) ApplySnapshot(peers []raftproto.Peer, iter raftproto.SnapIterator) error {
	return nil
}

func (mf *MetaRange) HandleFatalEvent(err *raft.FatalError) {

}

func (mf *MetaRange) HandleLeaderChange(leader uint64) {

}
