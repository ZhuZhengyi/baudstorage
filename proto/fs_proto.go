package proto

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
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
	PartitionID string `json:"partitionID"`
	Mode        uint32 `json:"mode"`
}

type CreateInodeResponse struct {
	Info *InodeInfo
}

type DeleteInodeRequest struct {
	Namespace   string `json:"namespace"`
	PartitionID string `json:"partitionID"`
	Inode       uint64 `json:"inode"`
}

type CreateDentryRequest struct {
	Namespace   string `json:"namespace"`
	PartitionID string `json:"partitionID"`
	ParentID    uint64 `json:"parentID"`
	Inode       uint64 `json:"inode"`
	Name        string `json:"name"`
	Mode        uint32 `json:"mode"`
}

type DeleteDentryRequest struct {
	Namespace   string `json:"namespace"`
	PartitionID string `json:"partitionID"`
	ParentID    uint64 `json:"parentID"`
	Name        string `json:"name"`
}

type DeleteDentryResponse struct {
	Inode uint64 `json:"inode"`
}

type OpenRequest struct {
	Namespace   string `json:"namespace"`
	PartitionID string `json:"partitionID"`
	Inode       uint64 `json:"inode"`
}

type LookupRequest struct {
	Namespace   string `json:"namespace"`
	PartitionID string `json:"partitionID"`
	ParentID    uint64 `json:"parentID"`
	Name        string `json:"name"`
}

type LookupResponse struct {
	Inode uint64 `json:"inode"`
	Mode  uint32 `json:"mode"`
}

type InodeGetRequest struct {
	Namespace   string `json:"namespace"`
	PartitionID string `json:"partitionID"`
	Inode       uint64 `json:"inode"`
}

type InodeGetResponse struct {
	Info *InodeInfo
}

type ReadDirRequest struct {
	Namespace   string `json:"namespace"`
	PartitionID string `json:"partitionID"`
	ParentID    uint64 `json:"parentID"`
}

type ReadDirResponse struct {
	Children []Dentry `json:"children"`
}

type AppendExtentKeyRequest struct {
	Namespace   string `json:"namespace"`
	PartitionID string `json:"partitionID"`
	Inode       uint64 `json:"inode"`
	Extent      ExtentKey
}

type GetExtentsRequest struct {
	Namespace   string `json:"namespace"`
	PartitionID string `json:"partitionID"`
	Inode       uint64 `json:"inode"`
}

type GetExtentsResponse struct {
	Extents []ExtentKey
}

var InvalidKey = errors.New("invalid key error")

type ExtentKey struct {
	VolId    uint32
	ExtentId uint64
	Size     uint32
	Crc      uint32
}

func (ek *ExtentKey) isEquare(k ExtentKey) bool {
	return ek.VolId == k.VolId && ek.ExtentId == k.ExtentId
}

func (k *ExtentKey) Marshal() (m string) {
	return fmt.Sprintf("%v_%v_%v_%v", k.VolId, k.ExtentId, k.Size, k.Crc)
}

func (k *ExtentKey) UnMarshal(m string) (err error) {
	var (
		size uint64
		crc  uint64
	)
	err = InvalidKey
	keyArr := strings.Split(m, "_")
	size, err = strconv.ParseUint(keyArr[2], 10, 64)
	if err != nil {
		return
	}
	crc, err = strconv.ParseUint(keyArr[3], 10, 64)
	if err != nil {
		return
	}
	vId, _ := strconv.ParseUint(keyArr[0], 10, 32)
	k.ExtentId, _ = strconv.ParseUint(keyArr[1], 10, 64)
	k.VolId = uint32(vId)
	k.Size = uint32(size)
	k.Crc = uint32(crc)

	return nil
}
