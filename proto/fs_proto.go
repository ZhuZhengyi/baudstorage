package proto

import (
	"time"
)

type InodeInfo struct {
	Inode      uint64    `json:"inode"`
	Type       uint32    `json:"type"`
	Name       string    `json:"name"`
	ParentId   uint64    `json:"parentId"`
	ModifyTime time.Time `json:"modify_time"`
	CreateTime time.Time `json:"create_time"`
	AccessTime time.Time `json:"access_time"`
	Extents    []string
}

type Dentry struct {
	Inode uint64 `json:"inode"`
	Type  uint32 `json:"type"`
	Name  string `json:"name"`
}

type CreateRequest struct {
	Namespace string `json:"namespace"`
	ParentId  uint64 `json:"parentId"`
	Name      string `json:"name"`
	Mode      uint32 `json:"mode"`
}

type CreateResponse struct {
	Status int `json:"status"`
	Dentry
}

type OpenRequest struct {
	Namespace string
	Inode     uint64
}

type OpenResponse struct {
	Status int
}

type LookupRequest struct {
	Namespace string
	ParentId  uint64
	Name      string
}

type LookupResponse struct {
	Status int
	Inode  uint64
	Mode   uint32
}

type InodeGetRequest struct {
	Namespace string
	Inode     uint64
}

type InodeGetResponse struct {
	Status int
	InodeInfo
}

type DeleteRequest struct {
	Namespace string
	ParentId  uint64
	Name      string
}

type DeleteResponse struct {
	Status int
}

type RenameRequest struct {
	Namespace   string
	SrcParentId uint64
	SrcName     string
	DstParentId uint64
	DstName     string
	Status      int
}

type RenameResponse struct {
	Status int
}

type ReadDirRequest struct {
	Namespace string
	ParentId  uint64
}

type ReadDirResponse struct {
	Children []Dentry
}
