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
	CltMaxRetry = 3
)

type BlockClient struct {
	conns       *pool.ConnPool
	vols        *sdk.VolGroupWrapper
	masterAddrs string
	isShutDown  bool
}

func NewBlockClient(maddrs string) *BlockClient {
	clt := &BlockClient{
		masterAddrs: maddrs,
		isShutDown:  false,
		conns:       pool.NewConnPool(),
		vols:        nil,
	}
	var err error
	clt.vols, err = sdk.NewVolGroupWraper(maddrs)
	if err != nil {
		return nil
	}
	return clt
}

func (clt *BlockClient) Release() {
	clt.isShutDown = true
}

func (clt *BlockClient) Write(data []byte) (key string, err error) {
	if len(data) == 0 {
		err = errors.New("empty data to write")
		return
	}
	var vol *sdk.VolGroup
	retried := 0
	for retried < CltMaxRetry {
		vol, err = clt.vols.GetWriteVol(nil)
		if err != nil {
			retried++
			continue
		}
		pkt := newWritePacket(data, vol)
		conn, err := clt.conns.Get(vol.Hosts[0])
		if err != nil {
			err = errors.New("connect to destination storage node failed, err: " + err.Error())
			clt.conns.Put(conn)
			retried++
			continue
		}
		err = pkt.WriteToConn(conn)
		if err != nil {
			err = errors.New("send data to destination storage node failed, err: " + err.Error())
			clt.conns.Put(conn)
			retried++
			continue
		}
		err = pkt.ReadFromConn(conn, ConnTimeOut)
		if err != nil {
			err = errors.New("receive data from destination storage node failed, err:" + err.Error())
			clt.conns.Put(conn)
			retried++
			continue
		}
		if pkt.Opcode != proto.OpOk {
			err = errors.New(string(pkt.Data[:]))
			clt.conns.Put(conn)
			retried++
			continue
		}
		key = marshalKey(pkt)
		break
	}
	return
}

func (clt *BlockClient) Read(key string) (data []byte, err error) {
	var (
		volId, size uint32
		offset      int64
		fileId      uint64
		vol         *sdk.VolGroup
	)
	volId, fileId, offset, size, _, err = unmarshalKey(key)
	if err != nil {
		return
	}
	vol, err = clt.vols.GetVol(volId)
	if err != nil {
		return
	}
	pkt := newReadPacket(volId, size, fileId, offset)

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
		err = pkt.ReadFromConn(conn, ConnTimeOut)
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

func (clt *BlockClient) Delete(key string) (err error) {
	var (
		volId, size uint32
		offset      int64
		fileId      uint64
		vol         *sdk.VolGroup
	)
	volId, fileId, offset, size, _, err = unmarshalKey(key)
	if err != nil {
		return
	}
	vol, err = clt.vols.GetVol(volId)
	if err != nil {
		return
	}
	pkt := newDelPacket(fileId, size, offset, vol)

	retried := 0
	for retried < CltMaxRetry {
		conn, err := clt.conns.Get(vol.Hosts[0])
		if err != nil {
			err = errors.New("connect to destination storage node failed, err: " + err.Error())
			retried++
			continue
		}
		err = pkt.WriteToConn(conn)
		if err != nil {
			err = errors.New("send request to destination storage node failed, err: " + err.Error())
			clt.conns.Put(conn)
			retried++
			continue
		}
		err = pkt.ReadFromConn(conn, ConnTimeOut)
		if err != nil {
			err = errors.New("receive response from destination storage node failed, err:" + err.Error())
			clt.conns.Put(conn)
			retried++
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
	}
	return
}
