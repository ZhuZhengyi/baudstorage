package stream

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
)

type ExtentKey struct {
	VolId    uint32
	ExtentId uint64
	Size     uint32
	Crc      uint32
}

type StreamKey struct {
	Inode   uint64
	Extents []ExtentKey
}

func NewStreamKey(ino uint64) *StreamKey {
	return &StreamKey{
		Inode: ino,
	}
}

func (sk *StreamKey) Put(k ExtentKey) {
	isFound := false
	for index:=0;index<len(sk.Extents);index++{
		if sk.Extents[index].VolId == k.VolId && sk.Extents[index].ExtentId == k.ExtentId {
			sk.Extents[index].Size = k.Size
			isFound = true
			return
		}
	}
	if !isFound {
		sk.Extents = append(sk.Extents, k)
	}

	return
}

func (sk *StreamKey) Size() (bytes uint64) {
	for _, okey := range sk.Extents {
		bytes += uint64(okey.Size)
	}
	return
}

// Range calls f sequentially for each key and value present in the extent key collection.
// If f returns false, range stops the iteration.
func (sk *StreamKey) Range(f func(i int, v ExtentKey) bool) {
	for i, v := range sk.Extents {
		if !f(i, v) {
			return
		}
	}
}

var (
	InvalidKey = errors.New("invalid key error")
)

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
