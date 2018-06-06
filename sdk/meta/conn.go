package meta

import (
	"fmt"
	"io"
	"net"

	"github.com/juju/errors"

	"github.com/tiglabs/baudstorage/proto"
	"github.com/tiglabs/baudstorage/util/log"
)

type MetaConn struct {
	conn net.Conn
	id   uint64 //PartitionID
	addr string //MetaNode addr
}

// Connection managements
//

func (mc *MetaConn) String() string {
	return fmt.Sprintf("partitionID(%v) addr(%v)", mc.id, mc.addr)
}

func (mw *MetaWrapper) getConn(mp *MetaPartition) (*MetaConn, error) {
	addr := mp.LeaderAddr
	//log.LogDebugf("Get connection: PartitionID(%v) addr(%v)\n", mp.PartitionID, addr)
	conn, err := mw.conns.Get(addr)
	if err != nil {
		log.LogErrorf("Get conn: addr(%v) err(%v)", addr, err)
		for _, addr = range mp.Members {
			conn, err = mw.conns.Get(addr)
			if err == nil {
				break
			}
			log.LogErrorf("Get conn: addr(%v) err(%v)", addr, err)
		}
	}

	if err != nil {
		return nil, err
	}

	//log.LogDebugf("Get connection: PartitionID(%v) addr(%v)\n", mp.PartitionID, addr)
	mc := &MetaConn{conn: conn, id: mp.PartitionID, addr: addr}
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
		return nil, errors.Annotatef(err, "Failed to write to conn: PartitionID(%v) addr(%v)", mc.id, mc.addr)
	}
	resp := proto.NewPacket()
	err = resp.ReadFromConn(mc.conn, proto.ReadDeadlineTime)
	if err != nil && err != io.EOF {
		return nil, errors.Annotatef(err, "Failed to read from conn: PartitionID(%v) addr(%v)", mc.id, mc.addr)
	}
	return resp, nil
}
