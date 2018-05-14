package metanode

import (
	"encoding/binary"
	"github.com/tiglabs/baudstorage/proto"
)

// Type alias.
type (
	// Master -> MetaNode  create metaRange request struct
	CreateMetaRangeReq = proto.CreateMetaRangeRequest
	// MetaNode -> Master create metaRange response struct
	CreateMetaRangeResp = proto.CreateMetaRangeResponse
	// Client -> MetaNode create inode request struct
	CreateInoReq = proto.CreateInodeRequest
	// MetaNode -> Client create inode response struct
	CreateInoResp = proto.CreateInodeResponse
	// Client -> MetaNode delete inode request struct
	DeleteInoReq = proto.DeleteInodeRequest
	// MetaNode -> Client delete inode response struct
	DeleteInoResp = proto.DeleteInodeResponse
	// Client -> MetaNode create dentry request struct
	CreateDentryReq = proto.CreateDentryRequest
	// MetaNode -> Client create dentry response struct
	CreateDentryResp = proto.CreateDentryResponse
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
	// MetaNode -> Client open file response struct
	OpenResp = proto.OpenResponse
)

type StoreKey = uint32

func (k StoreKey) GetBytes() (bytes []byte) {
	bytes = make([]byte, 4)
	binary.BigEndian.PutUint32(bytes, k)
	return
}

func (k *StoreKey) Parse(bytes []byte) {
	*k = binary.BigEndian.Uint32(bytes)
}

// For use when raft store and application apply
const (
	opCreateInode     StoreKey = iota
	opDeleteInode
	opCreateDentry
	opDeleteDentry
	opReadDir
	opOpen
	opCreateMetaRange
)
