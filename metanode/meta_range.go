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
	"github.com/tiglabs/baudstorage/raftstore"
	"github.com/tiglabs/baudstorage/sdk/stream"
)

// Errors
var (
	ErrInodeOutOfRange = errors.New("inode ID out of range.")
)

/* MetRangeConfig used by create metaRange and serialize
*  ID:		Consist with 'namespace_ID'. (Required when initialize)
*  Start:	Start inode ID of this range. (Required when initialize)
*  End:		End inode ID of this range. (Required when initialize)
*  Cursor:	Cursor ID value of inode what have been already assigned.
*  RaftGroupID:	Identity for raft group.Raft nodes in same raft group must have same groupID.
*  RaftServer:	Raft server instance.
 */
type MetaRangeConfig struct {
	ID            string              `json:"id"`
	Start         uint64              `json:"start"`
	End           uint64              `json:"end"`
	Cursor        uint64              `json:"-"`
	RootDir       string              `json:"-"`
	Peers         []proto.Peer        `json:"peers"`
	RaftGroupID   uint64              `json:"raftGroupID"`
	IsLeader      bool                `json:"-"`
	RaftPartition raftstore.Partition `json:"-"`
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
	// Load struct from meta
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

func (mr *MetaRange) StoreMeta() (err error) {
	// Store Meta to file
	metaFile := path.Join(mr.RootDir, "_meta")
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
	data, err := json.Marshal(mr)
	if err != nil {
		return
	}
	if _, err = fp.Write(data); err != nil {
		return
	}
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
func (mr *MetaRange) UpdatePeers(peers []proto.Peer) {
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

func (mr *MetaRange) CreateDentry(req *CreateDentryReq, p *Packet) (err error) {
	dentry := &Dentry{
		ParentId: req.ParentID,
		Name:     req.Name,
		Inode:    req.Inode,
		Type:     req.Mode,
	}
	val, err := json.Marshal(dentry)
	if err != nil {
		return
	}
	resp, err := mr.put(opCreateDentry, val)
	if err != nil {
		return
	}
	p.Resultcode = resp.(uint8)
	return
}

func (mr *MetaRange) DeleteDentry(req *DeleteDentryReq, p *Packet) (err error) {
	var resp *DeleteDentryResp
	dentry := &Dentry{
		ParentId: req.ParentID,
		Name:     req.Name,
	}
	val, err := json.Marshal(dentry)
	if err != nil {
		p.Resultcode = proto.OpErr
		return
	}
	r, err := mr.put(opDeleteDentry, val)
	if err != nil {
		p.Resultcode = proto.OpErr
		return
	}
	p.Resultcode = r.(uint8)
	if p.Resultcode == proto.OpOk {
		var reply []byte
		resp.Inode = dentry.Inode
		reply, err = json.Marshal(resp)
		p.PackOkWithBody(reply)
	}
	return
}

func (mr *MetaRange) CreateInode(req *CreateInoReq, p *Packet) (err error) {
	inoID, err := mr.nextInodeID()
	if err != nil {
		err = nil
		p.Resultcode = proto.OpInodeFullErr
		return
	}
	ts := time.Now()
	ino := &Inode{
		Inode:      inoID,
		Type:       req.Mode,
		CreateTime: ts,
		AccessTime: ts,
		ModifyTime: ts,
		Stream:     stream.NewStreamKey(inoID),
	}
	val, err := json.Marshal(ino)
	if err != nil {
		p.Resultcode = proto.OpErr
		return
	}
	r, err := mr.put(opCreateInode, val)
	if err != nil {
		return
	}
	p.Resultcode = r.(uint8)
	if p.Resultcode == proto.OpOk {
		var reply []byte
		resp := &CreateInoResp{}
		resp.Info.Inode = ino.Inode
		resp.Info.Type = ino.Type
		resp.Info.CreateTime = ino.CreateTime
		resp.Info.ModifyTime = ino.ModifyTime
		resp.Info.AccessTime = ino.AccessTime
		resp.Info.Size = ino.Size
		reply, err = json.Marshal(resp)
		if err != nil {
			p.Resultcode = proto.OpErr
			return
		}
		p.PackOkWithBody(reply)
	}
	return
}

func (mr *MetaRange) DeleteInode(req *DeleteInoReq, p *Packet) (err error) {
	ino := &Inode{
		Inode: req.Inode,
	}
	val, err := json.Marshal(ino)
	if err != nil {
		p.Resultcode = proto.OpErr
		return
	}
	r, err := mr.put(opDeleteInode, val)
	if err != nil {
		p.Resultcode = proto.OpErr
		return
	}
	p.Resultcode = r.(uint8)
	if p.Resultcode == proto.OpOk {
		var reply []byte
		resp := &DeleteDentryResp{
			Inode: ino.Inode,
		}
		reply, err = json.Marshal(resp)
		if err != nil {
			p.Resultcode = proto.OpErr
			return
		}
		p.PackOkWithBody(reply)
	}
	return
}

func (mr *MetaRange) PutStreamKey() {
	return
}

func (mr *MetaRange) ReadDir(req *ReadDirReq, p *Packet) (err error) {
	// TODO: Implement read dir operation.
	val, err := json.Marshal(req)
	if err != nil {
		p.Resultcode = proto.OpErr
		return
	}
	resp, err := mr.put(opReadDir, val)
	if err != nil {
		p.Resultcode = proto.OpErr
		return
	}
	var reply []byte
	reply, err = json.Marshal(resp)
	if err != nil {
		p.Resultcode = proto.OpErr
		return
	}
	p.PackOkWithBody(reply)
	return
}

func (mr *MetaRange) Open(req *OpenReq, p *Packet) (err error) {
	// TODO: Implement open operation.
	val, err := json.Marshal(req)
	if err != nil {
		p.Resultcode = proto.OpErr
		return
	}
	resp, err := mr.put(opOpen, val)
	if err != nil {
		p.Resultcode = proto.OpErr
		return
	}
	p.Resultcode = resp.(uint8)
	return
}

func (mr *MetaRange) put(op interface{}, body []byte) (r interface{}, err error) {
	r, err = mr.store.Put(op, body)
	return
}
