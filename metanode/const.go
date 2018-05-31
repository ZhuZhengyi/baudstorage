package metanode

import (
	"github.com/juju/errors"
	"github.com/tiglabs/baudstorage/proto"
)

// Type alias.
type (
	// Master -> MetaNode  create metaPartition request struct
	CreateMetaRangeReq = proto.CreateMetaPartitionRequest
	// MetaNode -> Master create metaPartition response struct
	CreateMetaRangeResp = proto.CreateMetaPartitionResponse
	// Client -> MetaNode create Inode request struct
	CreateInoReq = proto.CreateInodeRequest
	// MetaNode -> Client create Inode response struct
	CreateInoResp = proto.CreateInodeResponse
	// Client -> MetaNode delete Inode request struct
	DeleteInoReq = proto.DeleteInodeRequest
	// Client -> MetaNode create Dentry request struct
	CreateDentryReq = proto.CreateDentryRequest
	// Client -> MetaNode delete Dentry request struct
	DeleteDentryReq = proto.DeleteDentryRequest
	// MetaNode -> Client delete Dentry response struct
	DeleteDentryResp = proto.DeleteDentryResponse
	// Client -> MetaNode read dir request struct
	ReadDirReq = proto.ReadDirRequest
	// MetaNode -> Client read dir response struct
	ReadDirResp = proto.ReadDirResponse
	// MetaNode -> Client lookup
	LookupReq = proto.LookupRequest
	// Client -> MetaNode lookup
	LookupResp = proto.LookupResponse
	// Client -> MetaNode open file request struct
	OpenReq = proto.OpenRequest
	// Client -> MetaNode
	InodeGetReq = proto.InodeGetRequest
)

// For use when raftStore store and application apply
const (
	opCreateInode uint32 = iota
	opDeleteInode
	opCreateDentry
	opDeleteDentry
	opOpen
	opDeletePartition
	opUpdatePartition
	opOfflinePartition
	opExtentsAdd
)

var (
	masterAddrs   []string
	curMasterAddr string
	UMPKey        string
)

var (
	ErrNonLeader = errors.New("non leader")
	ErrNotLeader = errors.New("not leader")
)

const (
	defaultLogDir    = "logs"
	defaultMetaDir   = "metaDir"
	defaultRaftDir   = "raftDir"
	defaultPporfPort = 0
	defaultUMPKey    = "baudfs"
)

const (
	metaNodeURL     = "/metaNode/add"
	metaNodeGetName = "/admin/getIp"
)

// Configuration keys
const (
	cfgListen      = "listen"
	cfgMetaDir     = "metaDir"
	cfgRaftDir     = "raftDir"
	cfgMasterAddrs = "masterAddrs"
)
