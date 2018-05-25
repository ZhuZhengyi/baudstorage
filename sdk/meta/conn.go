package meta

import (
	"io"
	"net"

	"github.com/juju/errors"

	"github.com/tiglabs/baudstorage/proto"
)

type MetaConn struct {
	conn net.Conn
	id   uint64 //PartitionID
}

// Connection managements
//

func (mw *MetaWrapper) getConn(mp *MetaPartition) (*MetaConn, error) {
	addr := mp.LeaderAddr
	//TODO: deal with member 0 is not leader
	conn, err := mw.conns.Get(addr)
	if err != nil {
		return nil, err
	}

	mc := &MetaConn{conn: conn, id: mp.PartitionID}
	return mc, nil
}

func (mw *MetaWrapper) putConn(mc *MetaConn, err error) {
	if err != nil {
		mc.conn.Close()
	} else {
		mw.conns.Put(mc.conn)
	}
}

func (mw *MetaWrapper) connect(inode uint64) (*MetaConn, error) {
	mp := mw.getPartitionByInode(inode)
	if mp == nil {
		return nil, errors.New("No such meta group")
	}
	mc, err := mw.getConn(mp)
	if err != nil {
		return nil, err
	}
	return mc, nil
}

func (mc *MetaConn) send(req *proto.Packet) (*proto.Packet, error) {
	err := req.WriteToConn(mc.conn)
	if err != nil {
		return nil, err
	}
	resp := proto.NewPacket()
	err = resp.ReadFromConn(mc.conn, proto.ReadDeadlineTime)
	if err != nil && err != io.EOF {
		return nil, err
	}
	return resp, nil
}
