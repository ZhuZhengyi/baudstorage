package metanode

import (
	"fmt"
	"github.com/google/btree"
	"github.com/tiglabs/baudstorage/sdk/stream"
	"time"
)

// Dentry wraps necessary properties of `dentry` information in file system.
//
type Dentry struct {
	ParentId uint64 // Id value of parent inode.
	Name     string // Name of current dentry.
	Inode    uint64 // Id value of current inode.
	Type     uint32 // Dentry type.
}

// Less tests whether the current item is less than the given argument.
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

// GetKeyBytes is the bytes version of GetKey method which returns byte slice result.
func (d *Dentry) GetKeyBytes() (m []byte) {
	return []byte(d.GetKey())
}

// GetValueString returns string value of this dentry which consists of Inode and Type properties.
func (d *Dentry) GetValue() (m string) {
	return fmt.Sprintf("%d*%d", d.Inode, d.Type)
}

// GetValueBytes is the bytes version of GetValue method which returns byte slice result.
func (d *Dentry) GetValueBytes() (m []byte) {
	return []byte(d.GetValue())
}

// Inode wraps necessary properties of `inode` information in file system.
type Inode struct {
	Inode      uint64 // Inode Id
	Name       string
	Type       uint32
	Size       uint64
	AccessTime int64
	ModifyTime int64
	Stream     *stream.StreamKey
}

func NewInode(ino uint64, name string, t uint32) *Inode {
	ts := time.Now().Unix()
	return &Inode{
		Inode:      ino,
		Name:       name,
		Type:       t,
		AccessTime: ts,
		ModifyTime: ts,
		Stream:     stream.NewStreamKey(ino),
	}
}

func (i *Inode) Less(than btree.Item) bool {
	ino, ok := than.(*Inode)
	return ok && i.Inode < ino.Inode
}

// GetKey returns string value of key used as an index in the B-Tree consists of Inode property.
func (i *Inode) GetKey() (m string) {
	return fmt.Sprintf("%d", i.Inode)
}

// GetKeyBytes is the bytes version of GetKey method which returns byte slice result.
func (i *Inode) GetKeyBytes() (m []byte) {

	return []byte(i.GetKey())
}

// GetValue returns string value of this Inode which consists of Name, Size, AccessTime and
// ModifyTime properties and connected by '*'.
func (i *Inode) GetValue() (m string) {
	s := fmt.Sprintf("%s*%d*%d*%d", i.Name, i.Size, i.AccessTime, i.ModifyTime)
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

// GetValueBytes is the bytes version of GetValue method which returns byte slice result.
func (i *Inode) GetValueBytes() (m []byte) {
	return []byte(i.GetValue())
}
