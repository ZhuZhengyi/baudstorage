package metanode

import (
	"github.com/tiglabs/baudstorage/proto"
	"net"
)

func (m *metaManager) serveProxy(conn net.Conn, mp MetaPartition,
	p *Packet) (ok bool) {
	var (
		mConn      net.Conn
		leaderAddr string
		err        error
	)
	if leaderAddr, ok = mp.IsLeader(); ok {
		return
	}
	if leaderAddr == "" {
		err = ErrNonLeader
		p.PackErrorWithBody(proto.OpErr, nil)
		goto end
	}
	// Get Master Conn
	mConn, err = m.connPool.Get(leaderAddr)
	if err != nil {
		p.PackErrorWithBody(proto.OpErr, nil)
		goto end
	}
	// Send Master Conn
	if err = p.WriteToConn(mConn); err != nil {
		p.PackErrorWithBody(proto.OpErr, nil)
		goto end
	}
	// Read conn from master
	if err = p.ReadFromConn(mConn, proto.NoReadDeadlineTime); err != nil {
		p.PackErrorWithBody(proto.OpErr, nil)
		goto end
	}
	m.connPool.Put(mConn)
end:
	m.respondToClient(conn, p)
	return
}
