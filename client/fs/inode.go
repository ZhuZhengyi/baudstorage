package fs

import (
	"fmt"
	"sync"
	"syscall"
	"time"

	"bazil.org/fuse"

	"github.com/tiglabs/baudstorage/proto"
)

type Inode struct {
	// Fields specific to the filesystem
	sync.RWMutex
	ino     uint64
	super   *Super
	parent  *Dir
	blksize uint32

	// Fields get from meta server
	size     uint64
	mode     uint32 //Inode Type
	nlink    uint32
	blocks   uint64
	parentid uint64
	extents  []string

	ModifyTime time.Time
	CreateTime time.Time
	AccessTime time.Time
}

func InodeInit(inode *Inode, s *Super, ino uint64, p *Dir) {
	inode.super = s
	inode.ino = ino
	inode.parent = p
	inode.blksize = BLKSIZE_DEFAULT
}

func (s *Super) InodeGet(inode *Inode) error {
	status, info, err := s.meta.InodeGet(inode.ino)
	if err != nil {
		fmt.Println(err)
		return fuse.Errno(syscall.EAGAIN)
	}
	if status == int(proto.OpExistErr) {
		return fuse.Errno(syscall.EEXIST)
	} else if status != int(proto.OpOk) {
		return fuse.Errno(syscall.EIO)
	}

	fillInode(inode, info)
	return nil
}

func fillInode(inode *Inode, info *proto.InodeInfo) {
	inode.mode = info.Type
	inode.parentid = info.ParentID
	inode.extents = info.Extents
	inode.CreateTime = info.CreateTime
	inode.AccessTime = info.AccessTime
	inode.ModifyTime = info.ModifyTime
	//TODO: fill more fields
}
