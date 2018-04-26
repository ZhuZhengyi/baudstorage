package stream

import (
	"fmt"
	"github.com/tiglabs/baudstorage/proto"
	"github.com/tiglabs/baudstorage/sdk"
	"github.com/tiglabs/baudstorage/util/log"
	"sync"
	"time"
)

type ExtentClient struct {
	wraper     *sdk.VolGroupWraper
	writers    map[uint64]*StreamWriter
	writerLock sync.RWMutex
	readers    map[uint64]*StreamReader
	readerLock sync.Mutex
}

func NewExtentClient(logdir string, master string) (client *ExtentClient, err error) {
	client = new(ExtentClient)
	_, err = log.NewLog(logdir, "extentclient", log.DebugLevel)
	if err != nil {
		return nil, fmt.Errorf("init Log Failed[%v]", err.Error())
	}
	client.wraper, err = sdk.NewVolGroupWraper(master)
	if err != nil {
		return nil, fmt.Errorf("init volGroup Wrapper failed [%v]", err.Error())
	}
	client.writers = make(map[uint64]*StreamWriter)
	client.readers = make(map[uint64]*StreamReader)
	time.Sleep(time.Second * 5)

	return
}

func (client *ExtentClient) InitWrite(inode uint64, keyCh *chan ExtentKey) {
	stream := NewStreamWriter(client.wraper, inode, keyCh)
	client.writerLock.Lock()
	client.writers[inode] = stream
	client.writerLock.Unlock()

	return
}

func (client *ExtentClient) getStreamWriter(inode uint64) (stream *StreamWriter) {
	client.writerLock.RLock()
	stream = client.writers[inode]
	client.writerLock.RUnlock()

	return
}

func (client *ExtentClient) Write(inode uint64, data []byte) (write int, err error) {
	stream := client.getStreamWriter(inode)
	if stream == nil {
		return 0, fmt.Errorf("cannot init write stream")
	}
	return stream.write(data, len(data))
}

func (client *ExtentClient) Read(inode uint64, offset uint64, size uint32) (read int, data []byte, err error) {

	return
}

func (client *ExtentClient) Delete(keys []ExtentKey) (err error) {
	for _,k:=range keys{
		vol,err:=client.wraper.GetVol(k.VolId)
		if err!=nil {
			continue
		}
		client.delete(vol,k.ExtentId)
	}

	return nil
}

func (client *ExtentClient) delete(vol *sdk.VolGroup, extentId uint64) (err error) {
	connect, err := client.wraper.GetConnect(vol.Hosts[0])
	if err != nil {
		return
	}
	defer func() {
		if err == nil {
			client.wraper.PutConnect(connect)
		} else {
			connect.Close()
		}
	}()
	p := NewPacket(vol)
	p.Opcode = proto.OpMarkDelete
	p.VolID = vol.VolId
	p.FileID = extentId
	p.Arg = ([]byte)(vol.GetAllAddrs())
	p.Arglen = uint32(len(p.Arg))

	if err = p.WriteToConn(connect); err != nil {
		return
	}
	if err = p.ReadFromConn(connect, proto.ReadDeadlineTime); err != nil {
		return
	}
	extentId = p.FileID

	return
}
