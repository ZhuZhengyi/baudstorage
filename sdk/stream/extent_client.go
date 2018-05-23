package stream

import (
	"fmt"
	"sync"

	"github.com/juju/errors"
	"github.com/tiglabs/baudstorage/proto"
	"github.com/tiglabs/baudstorage/sdk"
	"github.com/tiglabs/baudstorage/util/log"
	"runtime"
)

type AppendExtentKeyFunc func(inode uint64, key proto.ExtentKey) error
type GetExtentsFunc func(inode uint64) ([]proto.ExtentKey, error)

type ExtentClient struct {
	wrapper         *sdk.VolGroupWrapper
	writers         map[uint64]*StreamWriter
	writerLock      sync.RWMutex
	readers         map[uint64]*StreamReader
	readerLock      sync.RWMutex
	referCnt        map[uint64]uint64
	referLock       sync.Mutex
	appendExtentKey AppendExtentKeyFunc
	getExtents      GetExtentsFunc
}

func NewExtentClient(logdir string, master string, appendExtentKey AppendExtentKeyFunc, getExtents GetExtentsFunc) (client *ExtentClient, err error) {
	runtime.GOMAXPROCS(runtime.NumCPU())
	client = new(ExtentClient)
	_, err = log.NewLog(logdir, "extentclient", log.DebugLevel)
	if err != nil {
		return nil, fmt.Errorf("init Log Failed[%v]", err.Error())
	}
	client.wrapper, err = sdk.NewVolGroupWraper(master)
	if err != nil {
		return nil, fmt.Errorf("init volGroup Wrapper failed [%v]", err.Error())
	}
	client.writers = make(map[uint64]*StreamWriter)
	client.readers = make(map[uint64]*StreamReader)
	client.referCnt = make(map[uint64]uint64)
	client.appendExtentKey = appendExtentKey
	client.getExtents = getExtents
	return
}

func (client *ExtentClient) InitWriteStream(inode uint64) (stream *StreamWriter) {
	stream = NewStreamWriter(client.wrapper, inode, client.appendExtentKey)
	client.writerLock.Lock()
	client.writers[inode] = stream
	client.writerLock.Unlock()

	return
}

func (client *ExtentClient) InitReadStream(inode uint64) (stream *StreamReader, err error) {
	stream, err = NewStreamReader(inode, client.wrapper, client.getExtents)
	if err != nil {
		return
	}
	client.readerLock.Lock()
	client.readers[inode] = stream
	client.readerLock.Unlock()

	return
}

func (client *ExtentClient) getStreamWriter(inode uint64) (stream *StreamWriter) {
	client.writerLock.RLock()
	stream = client.writers[inode]
	client.writerLock.RUnlock()
	if stream == nil {
		stream = client.InitWriteStream(inode)
	}

	return
}

func (client *ExtentClient) getStreamWriterForClose(inode uint64) (stream *StreamWriter) {
	client.writerLock.RLock()
	stream = client.writers[inode]
	client.writerLock.RUnlock()

	return
}

func (client *ExtentClient) getStreamReader(inode uint64) (stream *StreamReader, err error) {
	client.referLock.Lock()
	inodeReferCnt := client.referCnt[inode]
	client.referLock.Unlock()
	if inodeReferCnt == 0 {
		return nil, fmt.Errorf("Please Open inode[%v] Before ReadIt", inode)
	}
	client.readerLock.RLock()
	stream = client.readers[inode]
	client.readerLock.RUnlock()
	if stream == nil {
		stream, err = client.InitReadStream(inode)
	}
	return
}

func (client *ExtentClient) Write(inode uint64, data []byte) (write int, err error) {
	client.referLock.Lock()
	inodeReferCnt := client.referCnt[inode]
	client.referLock.Unlock()
	if inodeReferCnt == 0 {
		return 0, fmt.Errorf("Please Open inode[%v] Before Write", inode)
	}
	stream := client.getStreamWriter(inode)
	if stream == nil {
		return 0, fmt.Errorf("cannot init write stream")
	}
	request := &WriteRequest{data: data, size: len(data)}
	stream.requestCh <- request
	request = <-stream.replyCh
	err = request.err
	write = request.canWrite
	if err != nil {
		log.LogError(errors.ErrorStack(err))
	}
	return
}

func (client *ExtentClient) Open(inode uint64) {
	client.referLock.Lock()
	defer client.referLock.Unlock()
	inodeReferCnt, ok := client.referCnt[inode]
	if !ok {
		client.referCnt[inode] = 1
		return
	}
	client.referCnt[inode] = inodeReferCnt + 1
}

func (client *ExtentClient) Flush(inode uint64) (err error) {
	stream := client.getStreamWriter(inode)
	if stream == nil {
		return fmt.Errorf("cannot init write stream")
	}

	return stream.flushCurrExtentWriter()
}

func (client *ExtentClient) Close(inode uint64) (err error) {
	client.referLock.Lock()
	inodeReferCnt := client.referCnt[inode]
	if inodeReferCnt > 0 {
		client.referCnt[inode] = inodeReferCnt - 1

	}
	inodeReferCnt = client.referCnt[inode]
	client.referLock.Unlock()
	streamWriter := client.getStreamWriterForClose(inode)
	if streamWriter != nil {
		err = streamWriter.flushCurrExtentWriter()
	}
	if inodeReferCnt != 0 {
		return
	}

	if streamWriter != nil {
		err = streamWriter.close()
		client.writerLock.Lock()
		delete(client.writers, inode)
		client.writerLock.Unlock()
	}

	client.readerLock.RLock()
	streamReader := client.readers[inode]
	client.readerLock.RUnlock()
	if streamReader != nil {
		for _, reader := range streamReader.readers {
			reader.exitCh <- true
		}
		client.readerLock.Lock()
		delete(client.readers, inode)
		client.readerLock.Unlock()
		streamReader.close()
	}

	return
}

func (client *ExtentClient) Read(inode uint64, data []byte, offset int, size int) (read int, err error) {
	var stream *StreamReader
	if stream, err = client.getStreamReader(inode); err != nil {
		return
	}
	request := &ReadRequest{data: data, size: size, offset: offset}
	stream.requestCh <- request
	request = <-stream.replyCh
	err = request.err
	read = request.canRead

	return
}

func (client *ExtentClient) Delete(keys []proto.ExtentKey) (err error) {
	for _, k := range keys {
		vol, err := client.wrapper.GetVol(k.VolId)
		if err != nil {
			continue
		}
		client.delete(vol, k.ExtentId)
	}

	return nil
}

func (client *ExtentClient) delete(vol *sdk.VolGroup, extentId uint64) (err error) {
	connect, err := client.wrapper.GetConnect(vol.Hosts[0])
	if err != nil {
		return
	}
	defer func() {
		if err == nil {
			client.wrapper.PutConnect(connect)
		} else {
			connect.Close()
		}
	}()
	p := NewDeleteExtentPacket(vol, extentId)
	if err = p.WriteToConn(connect); err != nil {
		return
	}
	if err = p.ReadFromConn(connect, proto.ReadDeadlineTime); err != nil {
		return
	}

	return
}
