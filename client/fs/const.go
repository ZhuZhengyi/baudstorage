package fs

import (
	"fmt"

	"bazil.org/fuse"

	"github.com/tiglabs/baudstorage/proto"
)

const (
	ROOT_INO = proto.ROOT_INO
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
	StatusOK    = int(proto.OpOk)
	StatusExist = int(proto.OpExistErr)
	StatusNoEnt = int(proto.OpNotExistErr)
)

// TODO: log error
func ParseResult(status int, err error) error {
	if err != nil {
		fmt.Println(err)
		return fuse.EIO
	}

	var ret error
	switch status {
	case StatusOK:
		ret = nil
	case StatusExist:
		ret = fuse.EEXIST
	case StatusNoEnt:
		ret = fuse.ENOENT
	default:
		ret = fuse.EPERM
	}
	return ret
}
