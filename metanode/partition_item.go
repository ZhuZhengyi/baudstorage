package metanode

import (
	"encoding/json"
	"io"

	"github.com/google/btree"
)

type MetaItem struct {
	Op uint32 `json:"op"`
	K  []byte `json:"k"`
	V  []byte `json:"v"`
}

func (s *MetaItem) MarshalJson() ([]byte, error) {
	return json.Marshal(s)
}

func (s *MetaItem) MarshalBinary() (result []byte, err error) {
	panic("not implement yet")
}

func (s *MetaItem) UnmarshalJson(data []byte) error {
	return json.Unmarshal(data, s)
}

func (s *MetaItem) UnmarshalBinary(raw []byte) (err error) {
	panic("not implement yet")
}

func NewMetaPartitionSnapshot(op uint32, key, value []byte) *MetaItem {
	return &MetaItem{
		Op: op,
		K:  key,
		V:  value,
	}
}

type ItemIterator struct {
	applyID    uint64
	cur        int
	curItem    btree.Item
	inoLen     int
	inodeTree  *btree.BTree
	dentryLen  int
	dentryTree *btree.BTree
	total      int
}

func NewSnapshotIterator(applyID uint64, ino, den *btree.BTree) *ItemIterator {
	si := new(ItemIterator)
	si.applyID = applyID
	si.inodeTree = ino
	si.dentryTree = den
	si.cur = 1
	si.inoLen = ino.Len()
	si.dentryLen = den.Len()
	si.total = si.inoLen + si.dentryLen
	return si
}

func (si *ItemIterator) ApplyIndex() uint64 {
	return si.applyID
}

func (si *ItemIterator) Close() {
	return
}

func (si *ItemIterator) Next() (data []byte, err error) {
	if si.cur > si.total {
		err = io.EOF
		return
	}
	// ascend Inode tree
	if si.cur <= si.inoLen {
		si.inodeTree.AscendGreaterOrEqual(si.curItem, func(i btree.Item) bool {
			ino := i.(*Inode)
			if si.curItem == ino {
				return true
			}
			si.curItem = ino
			snap := NewMetaPartitionSnapshot(opCreateInode, ino.MarshalKey(),
				ino.MarshalValue())
			data, err = snap.MarshalJson()
			si.cur++
			return false
		})
		return
	}

	// ascend range dentry tree
	if si.cur == (si.inoLen + 1) {
		si.curItem = nil
	}
	si.dentryTree.AscendGreaterOrEqual(si.curItem, func(i btree.Item) bool {
		dentry := i.(*Dentry)
		if si.curItem == dentry {
			return true
		}
		si.curItem = dentry
		snap := NewMetaPartitionSnapshot(opCreateDentry, dentry.MarshalKey(),
			dentry.MarshalValue())
		data, err = snap.MarshalJson()
		si.cur++
		return false
	})
	return
}
