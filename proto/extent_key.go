package proto

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
)

var InvalidKey = errors.New("invalid key error")

type ExtentKey struct {
	PartitionId uint32
	ExtentId    uint64
	Size        uint32
	Crc         uint32
}

func (ek *ExtentKey) Equal(k ExtentKey) bool {
	return ek.PartitionId == k.PartitionId && ek.ExtentId == k.ExtentId
}

func (k *ExtentKey) Marshal() (m string) {
	return fmt.Sprintf("%v_%v_%v_%v", k.PartitionId, k.ExtentId, k.Size, k.Crc)
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
	k.PartitionId = uint32(vId)
	k.Size = uint32(size)
	k.Crc = uint32(crc)

	return nil
}
