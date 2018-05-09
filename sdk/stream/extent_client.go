package stream

import (
	"fmt"
	"sync"

	"github.com/tiglabs/baudstorage/proto"
	"github.com/tiglabs/baudstorage/sdk"
	"github.com/tiglabs/baudstorage/util/log"
)

type ExtentClient struct {
	wrapper           *sdk.VolGroupWraper
	writers           map[uint64]*StreamWriter
	writerLock        sync.RWMutex
	readers           map[uint64]*StreamReader
	readerLock        sync.RWMutex
	saveExtentKeyFn   func(inode uint64, key ExtentKey) (err error)
	updateExtentKeyFn func(inode uint64) (streamKey StreamKey, err error)
}

func NewExtentClient(logdir string, master string, saveExtentKeyFn func(inode uint64, key ExtentKey) (err error),
	updateExtentKeyFn func(inode uint64) (streamKey StreamKey, err error)) (client *ExtentClient, err error) {
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
	client.saveExtentKeyFn = saveExtentKeyFn
	client.updateExtentKeyFn = updateExtentKeyFn

	return
}

func (client *ExtentClient) InitWriteStream(inode uint64) (stream *StreamWriter) {
	stream = NewStreamWriter(client.wrapper, inode, client.saveExtentKeyFn)
	client.writerLock.Lock()
	client.writers[inode] = stream
	client.writerLock.Unlock()

	return
}

func (client *ExtentClient) InitReadStream(inode uint64) (stream *StreamReader, err error) {
	stream, err = NewStreamReader(inode, client.wrapper, client.updateExtentKeyFn)
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

func (client *ExtentClient) getStreamReader(inode uint64) (stream *StreamReader, err error) {
	client.readerLock.RLock()
	stream = client.readers[inode]
	client.readerLock.RUnlock()
	if stream == nil {
		stream, err = client.InitReadStream(inode)
	}
	return
}

func (client *ExtentClient) Write(inode uint64, data []byte) (write int, err error) {
	stream := client.getStreamWriter(inode)
	if stream == nil {
		return 0, fmt.Errorf("cannot init write stream")
	}
	return stream.write(data, len(data))
}

func (client *ExtentClient) Read(inode uint64, data []byte, offset int, size int) (read int, err error) {
	var stream *StreamReader
	if stream, err = client.getStreamReader(inode); err != nil {
		return
	}

	return stream.read(data, offset, size)
}

func (client *ExtentClient) Delete(keys []ExtentKey) (err error) {
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
