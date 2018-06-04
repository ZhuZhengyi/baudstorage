package fs

import (
	"syscall"
	"time"

	"bazil.org/fuse"

	"github.com/tiglabs/baudstorage/proto"
)

const (
	RootInode = proto.ROOT_INO
)

const (
	DIR_NLINK_DEFAULT     = 2
	REGULAR_NLINK_DEFAULT = 1
)

const (
	BLKSIZE_DEFAULT = uint32(1) << 12
)

const (
	ModeRegular = proto.ModeRegular
	ModeDir     = proto.ModeDir
)

const (
	LookupValidDuration = 120 * time.Second
	AttrValidDuration   = 120 * time.Second
)

const (
	InodeExpired           = int64(-1)
	DefaultInodeExpiration = 600 * time.Second
)

func ParseError(err error) fuse.Errno {
	switch v := err.(type) {
	case syscall.Errno:
		return fuse.Errno(v)
	default:
		return fuse.ENOSYS
	}
}

func ParseMode(mode uint32) fuse.DirentType {
	switch mode {
	case ModeDir:
		return fuse.DT_Dir
	default:
		return fuse.DT_File
	}
}
