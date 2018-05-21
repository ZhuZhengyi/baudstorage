package stream

import (
	"encoding/json"
	"sync"

	"github.com/tiglabs/baudstorage/proto"
)

type StreamKey struct {
	Inode   uint64
	Extents []proto.ExtentKey
	sync.Mutex
}

func NewStreamKey(ino uint64) *StreamKey {
	return &StreamKey{
		Inode: ino,
	}
}

func (sk *StreamKey) Marshal() (data []byte, err error) {
	return json.Marshal(sk)
}

func (sk *StreamKey) UnMarshal(data []byte) {
	json.Unmarshal(data, sk)
}

func (sk *StreamKey) Put(k proto.ExtentKey) {
	sk.Lock()
	defer sk.Unlock()
	lastKey := sk.Extents[len(sk.Extents)-1]
	if lastKey.VolId == k.VolId && lastKey.ExtentId == k.ExtentId && k.Size > lastKey.Size {
		sk.Extents[len(sk.Extents)-1].Size = k.Size
		return
	}
	sk.Extents = append(sk.Extents, k)

	return
}

func (sk *StreamKey) Size() (bytes uint64) {
	sk.Lock()
	defer sk.Unlock()
	for _, okey := range sk.Extents {
		bytes += uint64(okey.Size)
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
func (sk *StreamKey) Range(f func(i int, v proto.ExtentKey) bool) {
	sk.Lock()
	defer sk.Unlock()
	for i, v := range sk.Extents {
		if !f(i, v) {
			return
		}
	}
}
