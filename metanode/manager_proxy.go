package metanode

import (
	"net"

	"github.com/tiglabs/baudstorage/proto"
)

const (
	ForceCloseConnect=true
	NoCloseConnect=false
)

func (m *metaManager) serveProxy(conn net.Conn, mp MetaPartition,
	p *Packet) (ok bool) {
	var (
		mConn      *net.TCPConn
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
		m.connPool.Put(mConn,ForceCloseConnect)
		goto end
	}
	// Send Master Conn
	if err = p.WriteToConn(mConn); err != nil {
		p.PackErrorWithBody(proto.OpErr, nil)
		m.connPool.Put(mConn,ForceCloseConnect)
		goto end
	}
	// Read conn from master
	if err = p.ReadFromConn(mConn, proto.NoReadDeadlineTime); err != nil {
		p.PackErrorWithBody(proto.OpErr, nil)
		m.connPool.Put(mConn,ForceCloseConnect)
		goto end
	}
	m.connPool.Put(mConn,NoCloseConnect)
end:
	m.respondToClient(conn, p)
	return
}
