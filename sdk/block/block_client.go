package block

import (
	"errors"
	"github.com/tiglabs/baudstorage/proto"
	"github.com/tiglabs/baudstorage/sdk"
	"github.com/tiglabs/baudstorage/util/pool"
)

const (
	Version     = "1.0"
	ConnTimeOut = 20 //second
)

type BlockClient struct {
	conns       *pool.ConnPool
	vols        *sdk.VolGroupWraper
	masterAddrs []string
	isShutDown  bool
}

func NewBlockClient(maddrs []string) *BlockClient {
	clt := &BlockClient{
		masterAddrs: maddrs,
		isShutDown:  false,
		conns:       pool.NewConnPool(),
		vols:        &sdk.VolGroupWraper{},
	}
	if err := clt.vols.Init(clt.masterAddrs); err != nil {
		return nil
	}
	return clt
}

func (clt *BlockClient) Release() {
	clt.isShutDown = true
	clt.conns.Release()
}

func (clt *BlockClient) Write(data []byte) (key string, err error) {
	if len(data) == 0 {
		err = errors.New("empty data to write")
		return
	}
	var vol *sdk.VolGroup
	vol, err = clt.vols.GetWriteVol()
	if err != nil {
		return
	}
	pkt := newWritePacket(data, vol)
	conn, err := clt.conns.Get(vol.Hosts[0])
	if err != nil {
		err = errors.New("connect to destination storage node failed, err: " + err.Error())
		return
	}
	defer clt.conns.Put(conn)
	err = pkt.WriteToConn(conn)
	if err != nil {
		err = errors.New("send data to destination storage node failed, err: " + err.Error())
		return
	}
	err = pkt.ReadFromConn(conn, ConnTimeOut, 0)
	if err != nil {
		err = errors.New("receive data from destination storage node failed, err:" + err.Error())
		return
	}
	if pkt.Opcode != proto.OpOk {
		err = errors.New(string(pkt.Data[:]))
		return
	}
	key = marshalKey(pkt)

	return
}

func (clt *BlockClient) Read(key string) (data []byte, err error) {
	var (
		volId, size, crc uint32
		offset           int64
		fileId           uint64
		vol              *sdk.VolGroup
	)
	volId, fileId, offset, size, crc, err = unmarshalKey(key)
	if err != nil {
		return
	}
	vol, err = clt.vols.GetVol(uint64(volId))
	if err != nil {
		return
	}
	pkt := newReadPacket(volId, size, crc, fileId, offset)

	for _, host := range vol.Hosts {
		conn, err := clt.conns.Get(host)
		if err != nil {
			err = errors.New("connect to destination storage node failed, err: " + err.Error())
			continue
		}
		err = pkt.WriteToConn(conn)
		if err != nil {
			err = errors.New("send data to destination storage node failed, err: " + err.Error())
			clt.conns.Put(conn)
			continue
		}
		err = pkt.ReadFromConn(conn, ConnTimeOut, 0)
		if err != nil {
			err = errors.New("receive data from destination storage node failed, err:" + err.Error())
			clt.conns.Put(conn)
			continue
		}
		if pkt.Opcode != proto.OpOk {
			err = errors.New(string(pkt.Data[:]))
			clt.conns.Put(conn)
			continue
		}
		clt.conns.Put(conn)
		break
	}
	if err != nil {
		return
	}
	if pkt.Opcode != proto.OpOk {
		err = errors.New(string(pkt.Data[:]))
		return
	}
	data = pkt.Data
	return
}
