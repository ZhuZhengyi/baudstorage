package stream

import (
	"fmt"
	"github.com/juju/errors"
	"github.com/tiglabs/baudstorage/proto"
	"github.com/tiglabs/baudstorage/sdk/vol"
	"github.com/tiglabs/baudstorage/util/log"
	"io"
	"sync"
)

type ReadRequest struct {
	data    []byte
	offset  int
	size    int
	canRead int
	err     error
}

type StreamReader struct {
	inode      uint64
	wrapper    *vol.VolGroupWrapper
	readers    []*ExtentReader
	getExtents GetExtentsFunc
	extents    *proto.StreamKey
	fileSize   uint64
	requestCh  chan *ReadRequest
	replyCh    chan *ReadRequest
	exitCh     chan bool
	sync.Mutex
}

func NewStreamReader(inode uint64, wrapper *vol.VolGroupWrapper, getExtents GetExtentsFunc) (stream *StreamReader, err error) {
	stream = new(StreamReader)
	stream.inode = inode
	stream.wrapper = wrapper
	stream.getExtents = getExtents
	stream.requestCh = make(chan *ReadRequest, 1000)
	stream.replyCh = make(chan *ReadRequest, 1000)
	stream.exitCh = make(chan bool, 1)
	stream.extents = proto.NewStreamKey(inode)
	stream.extents.Extents, err = stream.getExtents(inode)
	if err != nil {
		return
	}
	var offset int
	var reader *ExtentReader
	stream.Lock()
	defer stream.Unlock()
	for _, key := range stream.extents.Extents {
		if reader, err = NewExtentReader(inode, offset, key, stream.wrapper); err != nil {
			return nil, errors.Annotatef(err, "NewStreamReader inode[%v] "+
				"key[%v] vol not found error", inode, key)
		}
		stream.readers = append(stream.readers, reader)
		offset += int(key.Size)
	}
	stream.fileSize = stream.extents.Size()
	go stream.server()
	return
}

func (stream *StreamReader) server() {
	for {
		select {
		case request := <-stream.requestCh:
			request.canRead, request.err = stream.read(request.data, request.offset, request.size)
			stream.replyCh <- request
		case <-stream.exitCh:
			return
		}
	}
}

func (stream *StreamReader) toString() (m string) {
	stream.Lock()
	defer stream.Unlock()
	return fmt.Sprintf("inode[%v] fileSize[%v] extents[%v] ",
		stream.inode, stream.fileSize, stream.extents)
}

func (stream *StreamReader) initCheck(offset, size int) (canread int, err error) {
	stream.Lock()
	defer stream.Unlock()
	if size > CFSEXTENTSIZE {
		return 0, io.EOF
	}
	if offset+size < int(stream.fileSize) {
		return size, nil
	}
	newStreamKey := proto.NewStreamKey(stream.inode)
	newStreamKey.Extents, err = stream.getExtents(stream.inode)

	if err == nil {
		err = stream.updateLocalReader(newStreamKey)
	}
	if err != nil {
		return 0, err
	}

	if offset >= int(stream.fileSize) {
		return 0, io.EOF
	}
	if offset+size > int(stream.fileSize) {
		return int(stream.fileSize) - offset, io.EOF
	}

	return size, nil
}

func (stream *StreamReader) close() {
	stream.exitCh <- true
	stream.exitCh <- true

}

func (stream *StreamReader) updateLocalReader(newStreamKey *proto.StreamKey) (err error) {
	var (
		newOffSet int
		r         *ExtentReader
	)
	readers := make([]*ExtentReader, 0)
	oldReaderCnt := len(stream.readers)
	for index, key := range newStreamKey.Extents {
		if index < oldReaderCnt-1 {
			newOffSet += int(key.Size)
			continue
		} else if index == oldReaderCnt-1 {
			stream.readers[index].updateKey(key)
			newOffSet += int(key.Size)
			//fmt.Printf("inode[%v] update from Metanode TO FILESIZE[%v]\n",
			//	stream.inode, newOffSet)
			continue
		} else if index > oldReaderCnt-1 {
			if r, err = NewExtentReader(stream.inode, newOffSet, key, stream.wrapper); err != nil {
				return errors.Annotatef(err, "NewStreamReader inode[%v] key[%v] "+
					"vol not found error", stream.inode, key)
			}
			readers = append(readers, r)
			newOffSet += int(key.Size)
			//fmt.Printf("inode[%v] update from Metanode TO FILESIZE[%v]\n", stream.inode, newOffSet)
			continue
		}
	}
	stream.fileSize = newStreamKey.Size()
	stream.extents = newStreamKey
	stream.readers = append(stream.readers, readers...)
	log.LogInfo(fmt.Sprintf("StreamReader update inode[%v] FileSize to [%v]",
		stream.inode, stream.fileSize))

	return nil
}

func (stream *StreamReader) read(data []byte, offset int, size int) (canRead int, err error) {
	var keyCanRead int
	keyCanRead, err = stream.initCheck(offset, size)
	if keyCanRead <= 0 || (err != nil && err != io.EOF) {
		return
	}
	readers, readerOffset, readerSize := stream.GetReader(offset, size)
	for index := 0; index < len(readers); index++ {
		r := readers[index]
		err = r.read(data[canRead:canRead+readerSize[index]], readerOffset[index], readerSize[index])
		if err != nil {
			err = errors.Annotatef(err, "UserRequest{inode[%v] FileSize[%v] "+
				"Offset[%v] Size[%v]} readers{ [%v] Offset[%v] Size[%v] occous error}",
				stream.inode, stream.fileSize, offset, size, r.toString(), readerOffset[index],
				readerSize[index])
			return canRead, err
		}
		canRead += readerSize[index]
	}

	return canRead, nil
}

func (stream *StreamReader) GetReader(offset, size int) (readers []*ExtentReader, readersOffsets []int, readersSize []int) {
	readers = make([]*ExtentReader, 0)
	readersOffsets = make([]int, 0)
	readersSize = make([]int, 0)
	orgOffset := offset
	orgSize := size
	stream.Lock()
	defer stream.Unlock()
	for _, r := range stream.readers {
		var (
			currReaderSize   int
			currReaderOffset int
		)
		if size <= 0 {
			break
		}
		r.Lock()
		if r.startInodeOffset > offset || r.endInodeOffset < offset {
			r.Unlock()
			continue
		}
		if r.endInodeOffset >= offset+size {
			currReaderOffset = offset - r.startInodeOffset
			currReaderSize = size
			offset += currReaderSize
			size -= currReaderSize
		} else {
			currReaderOffset = offset - r.startInodeOffset
			currReaderSize = int(r.key.Size) - currReaderOffset
			offset = r.endInodeOffset
			size = size - currReaderSize
		}
		if currReaderSize == 0 {
			continue
		}
		readersSize = append(readersSize, currReaderSize)
		readersOffsets = append(readersOffsets, currReaderOffset)
		readers = append(readers, r)
		log.LogDebugf("offset[%v] size[%v] reader[%v] readerOffsets[%v] "+
			"readerSize[%v]", orgOffset, orgSize, r.toString(), readersOffsets, readersSize)
		r.Unlock()
		if size <= 0 {
			break
		}
	}

	return
}
