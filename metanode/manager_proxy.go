package metanode

import (
	"github.com/tiglabs/baudstorage/proto"
	"github.com/tiglabs/baudstorage/util/log"
	"net"
	"time"
)

func (m *metaManager) serveProxy(conn net.Conn, mp MetaPartition,
	p *Packet) (ok bool) {
	var (
		mConn      net.Conn
		status     uint8
		leaderAddr string
		err        error
	)
	if leaderAddr, ok = mp.IsLeader(); ok {
		return
	}
	if leaderAddr == "" {
		err = ErrNonLeader
		status = proto.OpErr
		goto end
	}
	// Get Master Conn
	mConn, err = m.connPool.Get(leaderAddr)
	if err != nil {
		status = proto.OpErr
		goto end
	}
	defer func() {
		m.connPool.Put(mConn)
	}()
	// Send Master Conn
	if err = p.WriteToConn(mConn); err != nil {
		status = proto.OpErr
		goto end
	}
	// Read conn from master
	if err = p.ReadFromConn(mConn, proto.ReadDeadlineTime* time.
		Second); err != nil {
		status = proto.OpErr
		goto end
	}
end:
	p.PackErrorWithBody(status, nil)
	p.WriteToConn(conn)
	if err != nil {
		log.LogErrorf("proxy to master: %s", err.Error())
	}
	return
}
