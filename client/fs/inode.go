package fs

import (
	"sync"
	"time"
)

type Inode struct {
	// Fields specific to the filesystem
	sync.RWMutex
	ino     uint64
	super   *Super
	parent  *Dir
	blksize uint32

	// Fields get from meta server
	name     string
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

func InodeGet(inode *Inode) error {
	//TODO: get meta group according to ino
	//TODO: send InodeGetRequest to the meta node
	//TODO: fill inode according to InodeGetResponse
	return nil
}
