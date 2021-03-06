package proto

import (
	"bytes"
	"encoding/json"
	"fmt"
	"sync"
)

type StreamKey struct {
	Inode   uint64
	Extents []ExtentKey
	sync.Mutex
}

func (sk *StreamKey) String() string {
	buff := bytes.NewBuffer(make([]byte, 0))
	buff.WriteString("Inode{")
	buff.WriteString(fmt.Sprintf("Inode[%d]", sk.Inode))
	buff.WriteString(fmt.Sprintf("Extents[%v]", sk.Extents))
	buff.WriteString("}")
	return buff.String()
}

func NewStreamKey(ino uint64) *StreamKey {
	return &StreamKey{
		Inode: ino,
	}
}

func (sk *StreamKey) Marshal() (data []byte, err error) {
	return json.Marshal(sk)
}

func (sk *StreamKey) ToString() (m string) {
	data, _ := json.Marshal(sk)
	return string(data)
}

func (sk *StreamKey) UnMarshal(data []byte) {
	json.Unmarshal(data, sk)
}

func (sk *StreamKey) Put(k ExtentKey) {
	sk.Lock()
	defer sk.Unlock()
	if len(sk.Extents) == 0 {
		sk.Extents = append(sk.Extents, k)
		return
	}
	lastIndex := len(sk.Extents) - 1
	lastKey := sk.Extents[lastIndex]
	if lastKey.PartitionId == k.PartitionId && lastKey.ExtentId == k.ExtentId {
		if k.Size > lastKey.Size {
			sk.Extents[lastIndex].Size = k.Size
			return
		}
		return
	}
	var haveFileSize uint32
	for index, ek := range sk.Extents {
		if index != lastIndex && ek.PartitionId == k.PartitionId && ek.ExtentId == k.ExtentId {
			haveFileSize += ek.Size
		}
	}
	k.Size = k.Size - haveFileSize
	sk.Extents = append(sk.Extents, k)

	return
}

func (sk *StreamKey) Size() (bytes uint64) {
	sk.Lock()
	defer sk.Unlock()
	for _, ok := range sk.Extents {
		bytes += uint64(ok.Size)
	}
	return
}

func (sk *StreamKey) GetExtentLen() int {
	sk.Lock()
	defer sk.Unlock()
	return len(sk.Extents)
}

// Range calls f sequentially for each key and value present in the extent key collection.
// If f returns false, range stops the iteration.
func (sk *StreamKey) Range(f func(i int, v ExtentKey) bool) {
	sk.Lock()
	defer sk.Unlock()
	for i, v := range sk.Extents {
		if !f(i, v) {
			return
		}
	}
}
