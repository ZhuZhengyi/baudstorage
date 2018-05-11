package stream

import (
	"fmt"
	"github.com/juju/errors"
	"github.com/tiglabs/baudstorage/sdk"
	"io"
	"sync"
)

type StreamReader struct {
	inode          uint64
	wraper         *sdk.VolGroupWraper
	readers        []*ExtentReader
	updateExtentFn func(inode uint64) (sk *StreamKey, err error)
	extents        *StreamKey
	fileSize       uint64
	sync.Mutex
}

func NewStreamReader(inode uint64, wraper *sdk.VolGroupWraper, updateExtentFn func(inode uint64) (sk *StreamKey, err error)) (stream *StreamReader, err error) {
	stream = new(StreamReader)
	stream.inode = inode
	stream.wraper = wraper
	stream.updateExtentFn = updateExtentFn
	stream.extents = NewStreamKey(inode)
	stream.extents, err = stream.updateExtentFn(inode)
	if err != nil {
		return
	}
	var offset int
	var reader *ExtentReader
	for _, key := range stream.extents.Extents {
		if reader, err = NewExtentReader(offset, key, stream.wraper); err != nil {
			return nil, errors.Annotatef(err, "NewStreamReader inode[%v] key[%v] vol not found error", inode, key)
		}
		stream.readers = append(stream.readers, reader)
		offset += int(key.Size)
	}
	stream.fileSize = stream.extents.Size()
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
	return fmt.Sprintf("inode[%v] fileSize[%v] extents[%v] ", stream.inode, stream.fileSize, stream.extents)
}

func (stream *StreamReader) initCheck(offset, size int) (canread int, err error) {
	if size > CFSEXTENTSIZE {
		return 0, fmt.Errorf("read endOffset is So High")
	}
	if offset < int(stream.fileSize) {
		return size, nil
	}
	var newStreamKey *StreamKey
	newStreamKey, err = stream.updateExtentFn(stream.inode)
	stream.Lock()
	defer stream.Unlock()
	if err == nil {
		var newOffSet int
		var reader *ExtentReader
		for index, key := range newStreamKey.Extents {
			newOffSet += int(key.Size)
			if stream.isExsitExtentReader(key) {
				stream.readers[index].updateKey(key)
				continue
			}
			if reader, err = NewExtentReader(offset, key, stream.wraper); err != nil {
				return 0, errors.Annotatef(err, "NewStreamReader inode[%v] key[%v] vol not found error", stream.inode, key)
			}
			stream.readers = append(stream.readers, reader)
		}
		stream.fileSize = stream.extents.Size()
	}

	if offset > int(stream.fileSize) {
		return 0, fmt.Errorf("fileSize[%v] but read startOffset[%v]", stream.fileSize, offset)
	}
	if offset+size > int(stream.fileSize) {
		return int(stream.fileSize) - (offset + size), fmt.Errorf("fileSize[%v] but read startOffset[%v] endOffset[%v]",
			stream.fileSize, offset, size)
	}

	return size, nil
}

func (stream *StreamReader) read(data []byte, offset int, size int) (canRead int, err error) {
	var keyCanRead int
	keyCanRead, err = stream.initCheck(offset, size)
	if err != nil {
		err = io.EOF
	}
	if keyCanRead == 0 {
		return
	}
	readers, readerOffset, readerSize := stream.getReader(offset, size)
	for index := 0; index < len(readers); index++ {
		reader := readers[index]
		err = reader.read(data[canRead:canRead+readerSize[index]], readerOffset[index], readerSize[index])
		if err != nil {
			err = errors.Annotatef(err, "UserRequest{inode[%v] offset[%v] size[%v]} readers{"+
				"[%v] offset[%v] size[%v] occous error}", stream.inode, offset, size, reader.toString(), readerOffset[index],
				readerSize[index])
		}
		canRead += readerSize[index]
	}

	return
}

func (stream *StreamReader) getReader(offset, size int) (readers []*ExtentReader, readerOffset []int, readerSize []int) {
	readers = make([]*ExtentReader, 0)
	readerOffset = make([]int, 0)
	readerSize = make([]int, 0)
	//orgOffset:=offset
	//orgSize:=size
	for _, r := range stream.readers {
		if r.startInodeOffset <= offset && r.endInodeOffset >= offset+size {
			readers = append(readers, r)
			currReaderOffset := offset - r.startInodeOffset
			currReaderSize := size
			readerOffset = append(readerOffset, currReaderOffset)
			readerSize = append(readerSize, currReaderSize)
			size -= currReaderSize
			//log.Debug("inode[%v] orgOffset[%v] orgSize[%v] allocate reader[%v] " +
			//	"readerOffset[%v] readerSize[%v]\n",stream.inode,orgOffset,orgSize,r.toString(),currReaderOffset,currReaderSize)
		}
		if r.startInodeOffset <= offset && r.endInodeOffset <= offset+size {
			readers = append(readers, r)
			currReaderOffset := offset - r.startInodeOffset
			readerOffset = append(readerOffset, currReaderOffset)
			currReaderSize := (int(r.key.Size) - currReaderOffset)
			readerSize = append(readerSize, currReaderSize)
			//log.Debug("inode[%v] orgOffset[%v] orgSize[%v] allocate reader[%v] " +
			//	"readerOffset[%v] readerSize[%v]\n",stream.inode,orgOffset,orgSize,r.toString(),currReaderOffset,currReaderSize)
			offset += currReaderSize
			size -= currReaderSize
		}
		if size <= 0 {
			break
		}
	}

	return
}
