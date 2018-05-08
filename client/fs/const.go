package fs

import (
	"github.com/tiglabs/baudstorage/proto"
)

const (
	ROOT_INO = proto.ROOT_INO

	BLKSIZE_DEFAULT = uint32(1) << 12

	DIR_NLINK_DEFAULT     = 2
	REGULAR_NLINK_DEFAULT = 1
)
