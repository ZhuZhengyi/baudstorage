package metanode

import (
	"bytes"
	"encoding/binary"
	"strings"
	"time"

	"github.com/google/btree"
	"github.com/tiglabs/baudstorage/proto"
)

// Dentry wraps necessary properties of `Dentry` information in file system.
// Marshal key:
//  +--------+----------+------+
//  |  Item  | ParentId | Name |
//  +--------+----------+------+
//  |  Bytes |    8     | >=0  |
//  +--------+----------+------+
type Dentry struct {
	ParentId uint64 // FileIdId value of parent inode.
	Name     string // Name of current dentry.
	Inode    uint64 // FileIdId value of current inode.
	Type     uint32 // Dentry type.
}

// Marshal dentry item to bytes.
func (d *Dentry) Marshal() (result []byte, err error) {
	keyBytes := d.MarshalKey()
	valBytes := d.MarshalValue()
	keyLen := uint32(len(keyBytes))
	valLen := uint32(len(valBytes))
	buff := bytes.NewBuffer(make([]byte, 0))
	if err = binary.Write(buff, binary.BigEndian, keyLen); err != nil {
		return
	}
	if _, err = buff.Write(keyBytes); err != nil {
		return
	}
	if err = binary.Write(buff, binary.BigEndian, valLen); err != nil {

	}
	if _, err =  buff.Write(valBytes); err != nil {
		return
	}
	result = buff.Bytes()
	return
}

// Unmarshal dentry item from bytes.
func (d *Dentry) Unmarshal(raw []byte) (err error) {
	var (
		keyLen uint32
		valLen uint32
	)
	buff := bytes.NewBuffer(raw)
	if err = binary.Read(buff, binary.BigEndian, &keyLen); err != nil {
		return
	}
	keyBytes := make([]byte, keyLen)
	if _, err = buff.Read(keyBytes); err != nil {
		return
	}
	if err = d.UnmarshalKey(keyBytes); err != nil {
		return
	}
	if err = binary.Read(buff, binary.BigEndian, &valLen); err != nil {
		return
	}
	valBytes := make([]byte, valLen)
	if _, err = buff.Read(valBytes); err != nil {
		return
	}
	err = d.UnmarshalValue(valBytes)
	return
}

// Less tests whether the current Dentry item is less than the given one.
// This method is necessary fot B-Tree item implementation.
func (d *Dentry) Less(than btree.Item) (less bool) {
	dentry, ok := than.(*Dentry)
	less = ok && ((d.ParentId < dentry.ParentId) || ((d.ParentId == dentry.ParentId) && (d.Name < dentry.Name)))
	return
}

// MarshalKey is the bytes version of MarshalKey method which returns byte slice result.
func (d *Dentry) MarshalKey() (k []byte) {
	buff := bytes.NewBuffer(make([]byte, 0))
	if err := binary.Write(buff, binary.BigEndian, &d.ParentId); err != nil {
		panic(err)
	}
	buff.Write([]byte(d.Name))
	k = buff.Bytes()
	return
}

// UnmarshalKey unmarshal key from bytes.
func (d *Dentry) UnmarshalKey(k []byte) (err error) {
	buff := bytes.NewBuffer(k)
	if err = binary.Read(buff, binary.BigEndian, &d.ParentId); err != nil {
		return
	}
	d.Name = string(buff.Bytes())
	return
}

// MarshalValue marshal key to bytes.
func (d *Dentry) MarshalValue() (k []byte) {
	buff := bytes.NewBuffer(make([]byte, 0))
	if err := binary.Write(buff, binary.BigEndian, &d.Inode); err != nil {
		panic(err)
	}
	if err := binary.Write(buff, binary.BigEndian, &d.Type); err != nil {
		panic(err)
	}
	k = buff.Bytes()
	return
}

// UnmarshalValue unmarshal value from bytes.
func (d *Dentry) UnmarshalValue(val []byte) (err error) {
	buff := bytes.NewBuffer(val)
	if err = binary.Read(buff, binary.BigEndian, &d.Inode); err != nil {
		return
	}
	err = binary.Read(buff, binary.BigEndian, &d.Type)
	return
}

// Inode wraps necessary properties of `Inode` information in file system.
type Inode struct {
	Inode      uint64 // Inode ID
	Type       uint32
	Size       uint64
	Generation uint64
	CreateTime int64
	AccessTime int64
	ModifyTime int64
	Extents    *proto.StreamKey
}

// NewInode returns a new Inode instance pointer with specified Inode ID, name and Inode type code.
// The AccessTime and ModifyTime of new instance will be set to current time.
func NewInode(ino uint64, t uint32) *Inode {
	ts := time.Now().Unix()
	return &Inode{
		Inode:      ino,
		Type:       t,
		Generation: 1,
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

func (i *Inode) Marshal() (result []byte, err error) {
	keyBytes := i.MarshalKey()
	valBytes := i.MarshalValue()
	keyLen := uint32(len(keyBytes))
	valLen := uint32(len(valBytes))
	buff := bytes.NewBuffer(make([]byte, 0))
	if err = binary.Write(buff, binary.BigEndian, keyLen); err != nil {
		return
	}
	if _, err = buff.Write(keyBytes); err != nil {
		return
	}
	if err = binary.Write(buff, binary.BigEndian, valLen); err != nil {

	}
	if _, err =  buff.Write(valBytes); err != nil {
		return
	}
	result = buff.Bytes()
	return
}

func (i *Inode) Unmarshal(raw []byte) (err error) {
	var (
		keyLen uint32
		valLen uint32
	)
	buff := bytes.NewBuffer(raw)
	if err = binary.Read(buff, binary.BigEndian, &keyLen); err != nil {
		return
	}
	keyBytes := make([]byte, keyLen)
	if _, err = buff.Read(keyBytes); err != nil {
		return
	}
	if err = i.UnmarshalKey(keyBytes); err != nil {
		return
	}
	if err = binary.Read(buff, binary.BigEndian, &valLen); err != nil {
		return
	}
	valBytes := make([]byte, valLen)
	if _, err = buff.Read(valBytes); err != nil {
		return
	}
	err = i.UnmarshalValue(valBytes)
	return
}

// MarshalKey marshal key to bytes.
func (i *Inode) MarshalKey() (k []byte) {
	k = make([]byte, 8)
	binary.BigEndian.PutUint64(k, i.Inode)
	return
}

// UnmarshalKey unmarshal key from bytes.
func (i *Inode) UnmarshalKey(k []byte) (err error) {
	i.Inode = binary.BigEndian.Uint64(k)
	return
}

// MarshalValue marshal value to bytes.
func (i *Inode) MarshalValue() (val []byte) {
	var (
		err error
		extents []string
		extentsSeq []byte
		extentsSeqLen uint32
	)
	buff := bytes.NewBuffer(make([]byte, 0))
	if err = binary.Write(buff, binary.BigEndian, &i.Type); err != nil {
		panic(err)
	}
	if err = binary.Write(buff, binary.BigEndian, &i.Size); err != nil {
		panic(err)
	}
	if err = binary.Write(buff, binary.BigEndian, &i.Generation); err != nil {
		panic(err)
	}
	if err = binary.Write(buff, binary.BigEndian, &i.CreateTime); err != nil {
		panic(err)
	}
	if err = binary.Write(buff, binary.BigEndian, &i.AccessTime); err != nil {
		panic(err)
	}
	if err = binary.Write(buff, binary.BigEndian, &i.ModifyTime); err != nil {
		panic(err)
	}
	i.Extents.Range(func(i int, v proto.ExtentKey) bool {
		extents = append(extents, v.Marshal())
		return true
	})
	extentsSeq = []byte(strings.Join(extents, "*"))
	extentsSeqLen = uint32(len(extentsSeq))
	if err = binary.Write(buff, binary.BigEndian, &extentsSeqLen); err != nil {
		panic(err)
	}
	if _, err = buff.Write(extentsSeq); err != nil {
		panic(err)
	}
	val = buff.Bytes()
	return
}

// UnmarshalValue unmarshal value from bytes.
func (i *Inode) UnmarshalValue(val []byte) (err error) {
	buff := bytes.NewBuffer(val)
	if err = binary.Read(buff, binary.BigEndian, &i.Type); err != nil {
		return
	}
	if err = binary.Read(buff, binary.BigEndian, &i.Size); err != nil {
		return
	}
	if err = binary.Read(buff, binary.BigEndian, &i.Generation); err != nil {
		return
	}
	if err = binary.Read(buff, binary.BigEndian, &i.CreateTime); err != nil {
		return
	}
	if err = binary.Read(buff, binary.BigEndian, &i.AccessTime); err != nil {
		return
	}
	if err = binary.Read(buff, binary.BigEndian, &i.ModifyTime); err != nil {
		return
	}
	var (
		extents []string
		extentsSeq []byte
		extentsSeqLen uint32
	)
	if err = binary.Read(buff, binary.BigEndian, &extentsSeqLen); err != nil {
		return
	}
	if extentsSeqLen  == 0 {
		return
	}
	extentsSeq = make([]byte, extentsSeqLen)
	if _, err = buff.Read(extentsSeq); err != nil {
		return
	}
	extents = strings.Split(string(extentsSeq), "*")
	if i.Extents == nil {
		i.Extents = proto.NewStreamKey(i.Inode)
	} else {
		i.Extents.Inode = i.Inode
	}
	for _, value := range extents {
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
	i.Size = i.Extents.Size()
	i.ModifyTime = time.Now().Unix()
}
