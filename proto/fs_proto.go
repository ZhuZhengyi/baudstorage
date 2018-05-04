package proto

import (
	"time"
)

type InodeInfo struct {
	Inode      uint64    `json:"inode"`
	Type       uint32    `json:"type"`
	ParentID   uint64    `json:"parentID"`
	ModifyTime time.Time `json:"modify_time"`
	CreateTime time.Time `json:"create_time"`
	AccessTime time.Time `json:"access_time"`
	Extents    []string
}

type Dentry struct {
	Name  string `json:"name"`
	Inode uint64 `json:"inode"`
	Type  uint32 `json:"type"`
}

type OpResult struct {
	Status uint8 `json:"status"`
}

type CreateInodeRequest struct {
	Namespace string `json:"namespace"`
	Mode      uint32 `json:"mode"`
}

type CreateInodeResponse struct {
	OpResult
	Inode uint64 `json:"inode"`
}

type DeleteInodeRequest struct {
	Namespace string `json:"namespace"`
	Inode     uint64 `json:"inode"`
}

type DeleteInodeResponse struct {
	OpResult
}

type CreateDentryRequest struct {
	Namespace string `json:"namespace"`
	ParentID  uint64 `json:"parentID"`
	Inode     uint64 `json:"inode"`
	Name      string `json:"name"`
	Mode      uint32 `json:"mode"`
}

type CreateDentryResponse struct {
	OpResult
}

type DeleteDentryRequest struct {
	Namespace string `json:"namespace"`
	ParentID  uint64 `json:"parentID"`
	Name      string `json:"name"`
}

type DeleteDentryResponse struct {
	Status uint8  `json:"status"`
	Inode  uint64 `json:"inode"`
}

type OpenRequest struct {
	Namespace string `json:"namespace"`
	Inode     uint64 `json:"inode"`
}

type OpenResponse struct {
	OpResult
}

type LookupRequest struct {
	Namespace string `json:"namespace"`
	ParentID  uint64 `json:"parentID"`
	Name      string `json:"name"`
}

type LookupResponse struct {
	OpResult
	Inode uint64 `json:"inode"`
	Mode  uint32 `json:"mode"`
}

type InodeGetRequest struct {
	Namespace string `json:"namespace"`
	Inode     uint64 `json:"inode"`
}

type InodeGetResponse struct {
	OpResult
	Info *InodeInfo
}

type ReadDirRequest struct {
	Namespace string `json:"namespace"`
	ParentID  uint64 `json:"parentID"`
}

type ReadDirResponse struct {
	OpResult
	Children []Dentry `json:"children"`
}
