package stream

import (
	"fmt"
	"github.com/tiglabs/baudstorage/sdk"
	"io"
	"sync"
)

type StreamReader struct {
	inode     uint64
	wraper    *sdk.VolGroupWraper
	vol       *sdk.VolGroup
	readers   []*ExtentReader
	updateFn  func(inode uint64) (sk StreamKey, err error)
	streamKey StreamKey
	fileSize  uint64
	sync.Mutex
}

func NewStreamReader(inode uint64) (stream *StreamReader, err error) {
	stream = new(StreamReader)
	stream.inode = inode
	stream.streamKey, err = stream.updateFn(inode)
	if err != nil {
		return
	}
	var offset int
	for _, key := range stream.streamKey.Extents {
		stream.readers = append(stream.readers, NewExtentReader(offset, key))
		offset += int(key.Size)
	}
	stream.fileSize = stream.streamKey.Size()
	return
}

func (stream *StreamReader) isExsitExtentReader(k ExtentKey) (exsit bool) {
	for _, reader := range stream.readers {
		if reader.key.isEquare(k) {
			return true
		}
	}

	return
}

func (stream *StreamReader) toString() (m string) {
	return fmt.Sprintf("inode[%v] fileSize[%v] streamKey[%v] ", stream.inode, stream.fileSize, stream.streamKey)
}

func (stream *StreamReader) initCheck(offset, size int) (canread int, err error) {
	if size > CFSEXTENTSIZE {
		return 0, fmt.Errorf("read size is So High")
	}
	if offset < int(stream.fileSize) {
		return size, nil
	}
	var newStreamKey StreamKey
	newStreamKey, err = stream.updateFn(stream.inode)
	if err == nil {
		stream.streamKey = newStreamKey
		var newOffSet int
		for _, key := range stream.streamKey.Extents {
			newOffSet += int(key.Size)
			if stream.isExsitExtentReader(key) {
				continue
			}
			stream.readers = append(stream.readers, NewExtentReader(newOffSet, key))
		}
		stream.fileSize = stream.streamKey.Size()
	}

	if offset > int(stream.fileSize) {
		return 0, fmt.Errorf("fileSize[%v] but read offset[%v]", stream.fileSize, offset)
	}
	if offset+size > int(stream.fileSize) {
		return int(stream.fileSize) - (offset + size), fmt.Errorf("fileSize[%v] but read offset[%v] size[%v]",
			stream.fileSize, offset, size)
	}

	return size, nil
}

func (stream *StreamReader) read(data []byte, offset int, size int) (canRead int, err error) {
	canRead, err = stream.initCheck(offset, size)
	if err != nil {
		err = io.EOF
	}
	if canRead == 0 {
		return
	}

	return
}
