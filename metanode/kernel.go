package metanode

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/google/btree"
	"github.com/tiglabs/baudstorage/proto"
)

// Dentry wraps necessary properties of `Dentry` information in file system.
//
type Dentry struct {
	ParentId uint64 // FileIdId value of parent inode.
	Name     string // Name of current dentry.
	Inode    uint64 // FileIdId value of current inode.
	Type     uint32 // Dentry type.
}

// Dump Dentry item to bytes.
func (d *Dentry) Dump() (result []byte, err error) {
	return json.Marshal(d)
}

// Load Dentry item from bytes.
func (d *Dentry) Load(raw []byte) (err error) {
	return json.Unmarshal(raw, d)
}

// Less tests whether the current Dentry item is less than the given one.
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
//  | ParentId    | Name  | Key                          |
//  +----------------------------------------------------+
//  |           1 | demo1 | "                   1*demo1" |
//  | 84467440737 | demo2 | "         84467440737*demo2" |
//  +----------------------------------------------------+
func (d *Dentry) GetKey() (m string) {
	return fmt.Sprintf("%20d*%s", d.ParentId, d.Name)
}

// GetKeyBytes is the bytes version of GetKey method which returns byte slice result.
func (d *Dentry) GetKeyBytes() (m []byte) {
	return []byte(d.GetKey())
}

func (d *Dentry) ParseKeyBytes(k []byte) (err error) {
	key := string(k)
	_, err = fmt.Sscanf(key, "%d*%s", &d.ParentId, &d.Name)
	return
}

// GetValueString returns string value of this Dentry which consists of Inode and Type properties.
func (d *Dentry) GetValue() (m string) {
	return fmt.Sprintf("%d*%d", d.Inode, d.Type)
}

// GetValueBytes is the bytes version of GetValue method which returns byte slice result.
func (d *Dentry) GetValueBytes() (m []byte) {
	return []byte(d.GetValue())
}

func (d *Dentry) ParseValueBytes(val []byte) (err error) {
	value := string(val)
	_, err = fmt.Sscanf(value, "%d*%d", &d.Inode, &d.Type)
	return
}

// Inode wraps necessary properties of `Inode` information in file system.
type Inode struct {
	Inode      uint64 // Inode ID
	Type       uint32
	Size       uint64
	CreateTime int64
	AccessTime int64
	ModifyTime int64
	Extents    *proto.StreamKey
}

// Dump Inode item to bytes.
func (i *Inode) Dump() ([]byte, error) {
	return json.Marshal(i)
}

// Load Inode item from bytes.
func (i *Inode) Load(raw []byte) error {
	return json.Unmarshal(raw, i)
}

// NewInode returns a new Inode instance pointer with specified Inode ID, name and Inode type code.
// The AccessTime and ModifyTime of new instance will be set to current time.
func NewInode(ino uint64, t uint32) *Inode {
	ts := time.Now().Unix()
	return &Inode{
		Inode:      ino,
		Type:       t,
		CreateTime: ts,
		AccessTime: ts,
		ModifyTime: ts,
		Extents:    proto.NewStreamKey(ino),
	}
}

// Less tests whether the current Inode item is less than the given one.
// This method is necessary fot B-Tree item implementation.
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

func (i *Inode) ParseKeyBytes(k []byte) (err error) {
	key := string(k)
	inodeID, err := strconv.ParseUint(key, 10, 64)
	if err != nil {
		return
	}
	i.Inode = inodeID
	return
}

// GetValue returns string value of this Inode which consists of Name, Size, AccessTime and
// ModifyTime properties and connected by '*'.
func (i *Inode) GetValue() (m string) {
	s := fmt.Sprintf("%d*%d*%d*%d*%d", i.Type, i.Size, i.CreateTime, i.AccessTime, i.ModifyTime)
	var exts []string
	exts = append(exts, s)
	i.Extents.Range(func(i int, v proto.ExtentKey) bool {
		exts = append(exts, v.Marshal())
		return true
	})
	s = strings.Join(exts, "*")
	return s
}

// GetValueBytes is the bytes version of GetValue method which returns byte slice result.
func (i *Inode) GetValueBytes() (m []byte) {
	return []byte(i.GetValue())
}

func (i *Inode) ParseValueBytes(val []byte) (err error) {
	value := string(val)
	valSlice := strings.Split(value, "*")
	ttype, err := strconv.ParseUint(valSlice[0], 10, 32)
	if err != nil {
		return
	}
	size, err := strconv.ParseUint(valSlice[1], 10, 64)
	if err != nil {
		return
	}
	ctime, err := strconv.ParseInt(valSlice[2], 10, 64)
	if err != nil {
		return
	}
	atime, err := strconv.ParseInt(valSlice[3], 10, 64)
	if err != nil {
		return
	}
	mtime, err := strconv.ParseInt(valSlice[4], 10, 64)
	if err != nil {
		return
	}
	i.Type = uint32(ttype)
	i.Size = size
	i.CreateTime = ctime
	i.AccessTime = atime
	i.ModifyTime = mtime
	if len(valSlice) <= 5 {
		return
	}
	for _, value = range valSlice[6:] {
		var ext proto.ExtentKey
		if err = ext.UnMarshal(value); err != nil {
			return
		}
		i.Extents.Put(ext)
	}
	return
}

func (i *Inode) AppendExtents(ext proto.ExtentKey) {
	i.Extents.Put(ext)
}
