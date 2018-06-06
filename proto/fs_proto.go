package proto

import (
	"time"
)

const (
	ROOT_INO = uint64(1)
)

const (
	ModeRegular uint32 = iota
	ModeDir
)

type InodeInfo struct {
	Inode      uint64    `json:"inode"`
	Mode       uint32    `json:"mode"`
	Size       uint64    `json:"size"`
	ModifyTime time.Time `json:"modify_time"`
	CreateTime time.Time `json:"create_time"`
	AccessTime time.Time `json:"access_time"`
}

type Dentry struct {
	Name  string `json:"name"`
	Inode uint64 `json:"inode"`
	Type  uint32 `json:"type"`
}

type CreateInodeRequest struct {
	Namespace   string `json:"namespace"`
	PartitionID uint64 `json:"partitionID"`
	Mode        uint32 `json:"mode"`
}

type CreateInodeResponse struct {
	Info *InodeInfo
}

type DeleteInodeRequest struct {
	Namespace   string `json:"namespace"`
	PartitionID uint64 `json:"partitionID"`
	Inode       uint64 `json:"inode"`
}

type DeleteInodeResponse struct {
	Extents []ExtentKey
}

type CreateDentryRequest struct {
	Namespace   string `json:"namespace"`
	PartitionID uint64 `json:"partitionID"`
	ParentID    uint64 `json:"parentID"`
	Inode       uint64 `json:"inode"`
	Name        string `json:"name"`
	Mode        uint32 `json:"mode"`
}

type DeleteDentryRequest struct {
	Namespace   string `json:"namespace"`
	PartitionID uint64 `json:"partitionID"`
	ParentID    uint64 `json:"parentID"`
	Name        string `json:"name"`
}

type DeleteDentryResponse struct {
	Inode uint64 `json:"inode"`
}

type OpenRequest struct {
	Namespace   string `json:"namespace"`
	PartitionID uint64 `json:"partitionID"`
	Inode       uint64 `json:"inode"`
}

type LookupRequest struct {
	Namespace   string `json:"namespace"`
	PartitionID uint64 `json:"partitionID"`
	ParentID    uint64 `json:"parentID"`
	Name        string `json:"name"`
}

type LookupResponse struct {
	Inode uint64 `json:"inode"`
	Mode  uint32 `json:"mode"`
}

type InodeGetRequest struct {
	Namespace   string `json:"namespace"`
	PartitionID uint64 `json:"partitionID"`
	Inode       uint64 `json:"inode"`
}

type InodeGetResponse struct {
	Info *InodeInfo
}

type BatchInodeGetRequest struct {
	Namespace string   `json:"namespace"`
	Inode     []uint64 `json:"inodes"`
}

type BatchInodeGetResponse struct {
	Info []*InodeInfo `json:"infos"`
}

type ReadDirRequest struct {
	Namespace   string `json:"namespace"`
	PartitionID uint64 `json:"partitionID"`
	ParentID    uint64 `json:"parentID"`
}

type ReadDirResponse struct {
	Children []Dentry `json:"children"`
}

type AppendExtentKeyRequest struct {
	Namespace   string `json:"namespace"`
	PartitionID uint64 `json:"partitionID"`
	Inode       uint64 `json:"inode"`
	Extent      ExtentKey
}

type GetExtentsRequest struct {
	Namespace   string `json:"namespace"`
	PartitionID uint64 `json:"partitionID"`
	Inode       uint64 `json:"inode"`
}

type GetExtentsResponse struct {
	Extents []ExtentKey
}
