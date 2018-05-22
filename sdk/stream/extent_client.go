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
	sync.RWMutex
	vols            *sdk.VolGroupWrapper
	stream          map[uint64]*StreamWrapper
	refcnt          map[uint64]int
	appendExtentKey AppendExtentKeyFunc
	getExtents      GetExtentsFunc
}

type StreamWrapper struct {
	writer     *StreamWriter
	reader     *StreamReader
	initWriter sync.Once
	initReader sync.Once
}

func NewExtentClient(logdir string, master string, appendExtentKey AppendExtentKeyFunc, getExtents GetExtentsFunc) (*ExtentClient, error) {
	runtime.GOMAXPROCS(runtime.NumCPU())
	client := new(ExtentClient)
	_, err := log.NewLog(logdir, "extentclient", log.DebugLevel)
	if err != nil {
		return nil, fmt.Errorf("init Log Failed[%v]", err.Error())
	}
	client.vols, err = sdk.NewVolGroupWraper(master)
	if err != nil {
		return nil, fmt.Errorf("init volGroup Wrapper failed [%v]", err.Error())
	}
	client.stream = make(map[uint64]*StreamWrapper)
	client.refcnt = make(map[uint64]int)
	client.appendExtentKey = appendExtentKey
	client.getExtents = getExtents
	return client, nil
}

func (client *ExtentClient) Open(inode uint64) {
	client.Lock()
	defer client.Unlock()
	refcnt, ok := client.refcnt[inode]
	if ok {
		client.refcnt[inode] = refcnt + 1
	} else {
		client.refcnt[inode] = 1
		stream := new(StreamWrapper)
		client.stream[inode] = stream
	}
}

func (client *ExtentClient) Close(inode uint64) error {
	var err error

	writer := client.GetStreamWriter(inode)
	if writer != nil {
		err = writer.flushCurrExtentWriter()
		if err != nil {
			return err
		}
	}

	reader, err := client.GetStreamReader(inode)
	if err != nil {
		return err
	}

	client.Lock()
	defer client.Unlock()
	refcnt, ok := client.refcnt[inode]
	if !ok {
		return nil
	}
	if refcnt > 1 {
		client.refcnt[inode] = refcnt - 1
		return nil
	}

	delete(client.refcnt, inode)
	delete(client.stream, inode)

	if writer != nil {
		writer.close()
	}

	if reader != nil {
		for _, r := range reader.readers {
			r.exitCh <- true
		}
		reader.close()
	}
	return nil
}

func (client *ExtentClient) GetStreamWriter(inode uint64) *StreamWriter {
	client.RLock()
	stream, ok := client.stream[inode]
	client.RUnlock()
	if !ok {
		return nil
	}

	stream.initWriter.Do(func() {
		stream.writer = NewStreamWriter(client.vols, inode, client.appendExtentKey)
	})

	return stream.writer
}

func (client *ExtentClient) GetStreamReader(inode uint64) (*StreamReader, error) {
	var err error

	client.RLock()
	stream, ok := client.stream[inode]
	client.RUnlock()
	if !ok {
		return nil, fmt.Errorf("Please Open inode[%v] Before Reading", inode)
	}

	stream.initReader.Do(func() {
		stream.reader, err = NewStreamReader(inode, client.vols, client.getExtents)
	})

	if err != nil {
		return nil, err
	}
	return stream.reader, nil
}

func (client *ExtentClient) Write(inode uint64, data []byte) (write int, err error) {
	writer := client.GetStreamWriter(inode)
	if writer == nil {
		return 0, fmt.Errorf("cannot init stream writer")
	}
	request := &WriteRequest{data: data, size: len(data)}
	writer.requestCh <- request
	request = <-writer.replyCh
	err = request.err
	write = request.canWrite
	if err != nil {
		log.LogError(errors.ErrorStack(err))
	}
	return
}

func (client *ExtentClient) Flush(inode uint64) (err error) {
	writer := client.GetStreamWriter(inode)
	if writer == nil {
		return fmt.Errorf("cannot init stream writer")
	}

	return writer.flushCurrExtentWriter()
}

func (client *ExtentClient) Read(inode uint64, data []byte, offset int, size int) (read int, err error) {
	var reader *StreamReader
	if reader, err = client.GetStreamReader(inode); err != nil {
		return
	}
	request := &ReadRequest{data: data, size: size, offset: offset}
	reader.requestCh <- request
	request = <-reader.replyCh
	err = request.err
	read = request.canRead
	return
}

func (client *ExtentClient) Delete(keys []proto.ExtentKey) (err error) {
	for _, k := range keys {
		vol, err := client.vols.GetVol(k.VolId)
		if err != nil {
			continue
		}
		client.delete(vol, k.ExtentId)
	}

	return nil
}

func (client *ExtentClient) delete(vol *sdk.VolGroup, extentId uint64) (err error) {
	connect, err := client.vols.GetConnect(vol.Hosts[0])
	if err != nil {
		return
	}
	defer func() {
		if err == nil {
			client.vols.PutConnect(connect)
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
