package proto

import (
	"time"
)

type InodeInfo struct {
	Inode      uint64    `json:"inode"`
	Type       uint32    `json:"type"`
	Name       string    `json:"name"`
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

type CreateInodeRequest struct {
	Namespace string `json:"namespace"`
	Name      string `json:"name"`
	Mode      uint32 `json:"mode"`
}

type CreateInodeResponse struct {
	Status int    `json:"status"`
	Inode  uint64 `json:"inode"`
}

type DeleteInodeRequest struct {
	Namespace string `json:"namespace"`
	Inode     uint64 `json:"inode"`
}

type DeleteInodeResponse struct {
	Status int `json:"status"`
}

type CreateDentryRequest struct {
	Namespace string `json:"namespace"`
	ParentID  uint64 `json:"parentID"`
	Inode     uint64 `json:"inode"`
	Name      string `json:"name"`
	Mode      uint32 `json:"mode"`
}

type CreateDentryResponse struct {
	Status int `json:"status"`
}

type DeleteDentryRequest struct {
	Namespace string `json:"namespace"`
	ParentID  uint64 `json:"parentID"`
	Name      string `json:"name"`
}

type DeleteDentryResponse struct {
	Status int    `json:"status"`
	Inode  uint64 `json:"inode"`
}

type UpdateInodeNameRequest struct {
	Namespace string `json:"namespace"`
	Inode     uint64 `json:"inode"`
	Name      string `json:"name"`
}

type UpdateInodeNameResponse struct {
	Status int `json:"status"`
}

type OpenRequest struct {
	Namespace string `json:"namespace"`
	Inode     uint64 `json:"inode"`
}

type OpenResponse struct {
	Status int `json:"status"`
}

type LookupRequest struct {
	Namespace string `json:"namespace"`
	ParentID  uint64 `json:"parentID"`
	Name      string `json:"name"`
}

type LookupResponse struct {
	Status int    `json:"status"`
	Inode  uint64 `json:"inode"`
	Mode   uint32 `json:"mode"`
}

type InodeGetRequest struct {
	Namespace string `json:"namespace"`
	Inode     uint64 `json:"inode"`
}

type InodeGetResponse struct {
	Status int `json:"status"`
	InodeInfo
}

type ReadDirRequest struct {
	Namespace string `json:"namespace"`
	ParentID  uint64 `json:"parentID"`
}

type ReadDirResponse struct {
	Children []Dentry
}
