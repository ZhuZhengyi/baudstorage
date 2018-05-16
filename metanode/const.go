package metanode

import (
	"errors"
	"github.com/tiglabs/baudstorage/proto"
)

// Type alias.
type (
	// Master -> MetaNode  create metaPartition request struct
	CreateMetaRangeReq = proto.CreateMetaPartitionRequest
	// MetaNode -> Master create metaPartition response struct
	CreateMetaRangeResp = proto.CreateMetaPartitionResponse
	// Client -> MetaNode create inode request struct
	CreateInoReq = proto.CreateInodeRequest
	// MetaNode -> Client create inode response struct
	CreateInoResp = proto.CreateInodeResponse
	// Client -> MetaNode delete inode request struct
	DeleteInoReq = proto.DeleteInodeRequest
	// Client -> MetaNode create dentry request struct
	CreateDentryReq = proto.CreateDentryRequest
	// Client -> MetaNode delete dentry request struct
	DeleteDentryReq = proto.DeleteDentryRequest
	// MetaNode -> Client delete dentry response struct
	DeleteDentryResp = proto.DeleteDentryResponse
	// Client -> MetaNode read dir request struct
	ReadDirReq = proto.ReadDirRequest
	// MetaNode -> Client read dir response struct
	ReadDirResp = proto.ReadDirResponse
	// Client -> MetaNode open file request struct
	OpenReq = proto.OpenRequest
)

// For use when raft store and application apply
const (
	opCreateInode uint32 = iota
	opDeleteInode
	opCreateDentry
	opDeleteDentry
	opReadDir
	opOpen
	opCreateMetaRange
)

var (
	ErrNonLeader = errors.New("non leader")
	ErrNotLeader = errors.New("not leader")
)
