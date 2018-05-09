package metanode

import (
	"fmt"
	"time"

	"github.com/google/btree"
	"github.com/tiglabs/baudstorage/sdk/stream"
	"strconv"
)

// Dentry wraps necessary properties of `dentry` information in file system.
//
type Dentry struct {
	ParentId uint64 // Id value of parent inode.
	Name     string // Name of current dentry.
	Inode    uint64 // Id value of current inode.
	Type     uint32 // Dentry type.
}

// Less tests whether the current dentry item is less than the given one.
// This method is necessary fot B-Tree item implementation.
func (d *Dentry) Less(than btree.Item) (less bool) {
	dentry, ok := than.(*Dentry)
	less = ok && d.GetKey() < dentry.GetKey()
	return
}

// GetKeyString returns string value of key used as an index in the B-Tree consists
// of ParentId and Name properties and connected by '*'.
// Since the type of `InodeId` is uint64 and the maximum value is 1.844674407371e+19,
// so the `ParentId` in key uses 20-bit character alignment to support fuzzy retrieval.
// Example:
//  +----------------------------------------------------+
//  | ParentId    | Name  | ExtentKey                    |
//  +----------------------------------------------------+
//  |           1 | demo1 | "                   1*demo1" |
//  | 84467440737 | demo2 | "         84467440737*demo2" |
//  +----------------------------------------------------+
func (d *Dentry) GetKey() (m string) {
	return fmt.Sprintf("%10d*%s", d.ParentId, d.Name)
}

func (d *Dentry) ParseKey(key string) (err error) {
	_, err = fmt.Sscanf(key, "%d*%s", &d.ParentId, d.Name)
	return
}

// GetKeyBytes is the bytes version of GetKey method which returns byte slice result.
func (d *Dentry) GetKeyBytes() (m []byte) {
	return []byte(d.GetKey())
}

// GetValueString returns string value of this dentry which consists of Inode and Type properties.
func (d *Dentry) GetValue() (m string) {
	return fmt.Sprintf("%d*%d", d.Inode, d.Type)
}

func (d *Dentry) ParseValue(value string) (err error) {
	_, err = fmt.Sscanf(value, "%d*%d", &d.Inode, &d.Type)
	return
}

// GetValueBytes is the bytes version of GetValue method which returns byte slice result.
func (d *Dentry) GetValueBytes() (m []byte) {
	return []byte(d.GetValue())
}

// Inode wraps necessary properties of `inode` information in file system.
type Inode struct {
	Inode      uint64 // Inode ID
	Type       uint32
	Size       uint64
	AccessTime int64
	ModifyTime int64
	Stream     *stream.StreamKey
}

// NewInode returns a new inode instance pointer with specified inode ID, name and inode type code.
// The AccessTime and ModifyTime of new instance will be set to current time.
func NewInode(ino uint64, t uint32) *Inode {
	ts := time.Now().Unix()
	return &Inode{
		Inode:      ino,
		Type:       t,
		AccessTime: ts,
		ModifyTime: ts,
		Stream:     stream.NewStreamKey(ino),
	}
}

// Less tests whether the current inode item is less than the given one.
// This method is necessary fot B-Tree item implementation.
func (i *Inode) Less(than btree.Item) bool {
	ino, ok := than.(*Inode)
	return ok && i.Inode < ino.Inode
}

// GetKey returns string value of key used as an index in the B-Tree consists of Inode property.
func (i *Inode) GetKey() (m string) {
	return fmt.Sprintf("%d", i.Inode)
}

func (i *Inode) ParseKey(key string) (err error) {
	inodeID, err := strconv.ParseUint(key, 10, 64)
	if err != nil {
		return
	}
	i.Inode = inodeID
	return
}

// GetKeyBytes is the bytes version of GetKey method which returns byte slice result.
func (i *Inode) GetKeyBytes() (m []byte) {

	return []byte(i.GetKey())
}

// GetValue returns string value of this Inode which consists of Name, Size, AccessTime and
// ModifyTime properties and connected by '*'.
func (i *Inode) GetValue() (m string) {
	s := fmt.Sprintf("%d*%d*%d*%d", i.Type, i.Size, i.AccessTime, i.ModifyTime)
	i.Stream.Range(func(index int, extentKey stream.ExtentKey) bool {
		if uint64(index) == i.Stream.Size() {
			s += extentKey.Marshal()
		} else {
			s += "*"
			s += extentKey.Marshal()
		}
		return true
	})
	return s
}

func (i *Inode) ParseValue(value string) (err error) {
	_, err = fmt.Sscanf(value, "%d*%d*%d*%d", &i.Type, &i.Size, &i.AccessTime, &i.ModifyTime)
	return
}

// GetValueBytes is the bytes version of GetValue method which returns byte slice result.
func (i *Inode) GetValueBytes() (m []byte) {
	return []byte(i.GetValue())
}
