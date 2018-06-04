package stream

import (
	"fmt"
	"github.com/juju/errors"
	"github.com/tiglabs/baudstorage/proto"
	"github.com/tiglabs/baudstorage/sdk/data"
	"github.com/tiglabs/baudstorage/util/log"
	"math/rand"
	"sync"
	"time"
)

type ExtentReader struct {
	inode            uint64
	startInodeOffset int
	endInodeOffset   int
	dp               *data.DataPartion
	key              proto.ExtentKey
	wrapper          *data.DataPartionWrapper
	sync.Mutex
}

func NewExtentReader(inode uint64, inInodeOffset int, key proto.ExtentKey,
	wrapper *data.DataPartionWrapper) (reader *ExtentReader, err error) {
	reader = new(ExtentReader)
	reader.dp, err = wrapper.GetDataPartion(key.PartionId)
	if err != nil {
		return
	}
	reader.inode = inode
	reader.key = key
	reader.startInodeOffset = inInodeOffset
	reader.endInodeOffset = reader.startInodeOffset + int(key.Size)
	reader.wrapper = wrapper

	return
}

func (reader *ExtentReader) read(data []byte, offset, size int) (err error) {
	if size <= 0 {
		return
	}
	reader.Lock()
	p := NewReadPacket(reader.key, offset, size)
	reader.Unlock()
	err = reader.readDataFromVol(p, data)

	return
}

func (reader *ExtentReader) readDataFromVol(p *Packet, data []byte) (err error) {
	rand.Seed(time.Now().UnixNano())
	index := rand.Intn(int(reader.dp.ReplicaNum))
	host := reader.dp.Hosts[index]
	if _, err = reader.readDataFromHost(p, host, data); err != nil {
		log.LogError(err.Error())
		goto FORLOOP
	}
	return

FORLOOP:
	for _, host := range reader.dp.Hosts {
		_, err = reader.readDataFromHost(p, host, data)
		if err == nil {
			return
		} else {
			log.LogError(err.Error())
		}
	}

	return
}

func (reader *ExtentReader) readDataFromHost(p *Packet, host string, data []byte) (acatualReadSize int, err error) {
	expectReadSize := int(p.Size)
	conn, err := reader.wrapper.GetConnect(host)
	if err != nil {
		return 0, errors.Annotatef(err, reader.toString()+
			"readDataFromHost dp[%v] cannot get  connect from host[%v] request[%v] ",
			reader.key.PartionId, host, p.GetUniqLogId())

	}
	defer func() {
		if err != nil {
			log.LogError(err.Error())
			conn.Close()
		} else {
			reader.wrapper.PutConnect(conn)
		}
	}()
	if err = p.WriteToConn(conn); err != nil {
		err = errors.Annotatef(err, reader.toString()+"readDataFromHost host[%v] error request[%v]",
			host, p.GetUniqLogId())
		return 0, err
	}
	for {
		if acatualReadSize >= expectReadSize {
			return acatualReadSize, err
		}
		err = p.ReadFromConn(conn, proto.ReadDeadlineTime)
		if err != nil {
			err = errors.Annotatef(err, reader.toString()+"readDataFromHost host[%v]  error reqeust[%v]",
				host, p.GetUniqLogId())
			return acatualReadSize, err

		}
		if p.ResultCode != proto.OpOk {
			err = errors.Annotatef(fmt.Errorf(string(p.Data[:p.Size])),
				reader.toString()+"readDataFromHost host [%v] request[%v] reply[%v]",
				host, p.GetUniqLogId(), p.GetUniqLogId())
			return acatualReadSize, err
		}
		copy(data[acatualReadSize:acatualReadSize+int(p.Size)], p.Data[:p.Size])
		acatualReadSize += int(p.Size)
		if acatualReadSize >= expectReadSize {
			return acatualReadSize, err
		}

	}
	return acatualReadSize, nil
}

func (reader *ExtentReader) updateKey(key proto.ExtentKey) (update bool) {
	reader.Lock()
	defer reader.Unlock()
	if !(key.PartionId == reader.key.PartionId && key.ExtentId == reader.key.ExtentId) {
		return
	}
	if key.Size <= reader.key.Size {
		return
	}
	reader.key = key
	reader.endInodeOffset = reader.startInodeOffset + int(key.Size)

	return true
}

func (reader *ExtentReader) toString() (m string) {
	return fmt.Sprintf("inode[%v] extentKey[%v] start[%v] end[%v]", reader.inode,
		reader.key.Marshal(), reader.startInodeOffset, reader.endInodeOffset)
}
