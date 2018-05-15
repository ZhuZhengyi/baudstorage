package pool

import (
	"net"
	"strings"
	"sync"
	"time"
)

type ConnPool struct {
	sync.Mutex
	pools map[string]*ChannelPool
}

func NewConnPool() (connP *ConnPool) {
	return &ConnPool{pools: make(map[string]*ChannelPool)}
}

func (connP *ConnPool) Get(targetAddr string) (c net.Conn, err error) {
	var obj interface{}
	connP.Lock()
	pool, ok := connP.pools[targetAddr]
	connP.Unlock()
	factory := func(addr interface{}) (interface{}, error) {
		var connect *net.TCPConn
		conn, err := net.DialTimeout("tcp", addr.(string), time.Second)
		if err == nil {
			connect, _ = conn.(*net.TCPConn)
			connect.SetKeepAlive(true)
			connect.SetNoDelay(true)
		}

		return connect, err
	}
	close := func(v interface{}) error { return v.(net.Conn).Close() }
	if !ok {
		poolConfig := &PoolConfig{
			InitialCap:  5,
			MaxCap:      300,
			Factory:     factory,
			Close:       close,
			IdleTimeout: time.Minute * 30,
			Para:        targetAddr,
		}
		pool, err = NewChannelPool(poolConfig)
		if err != nil {
			conn, err := factory(targetAddr)
			return conn.(net.Conn), err
		}
		connP.Lock()
		connP.pools[targetAddr] = pool
		connP.Unlock()
	}

	if obj, err = pool.Get(); err != nil {
		conn, err := factory(targetAddr)
		return conn.(net.Conn), err
	}
	c = obj.(net.Conn)

	return
}

func (connP *ConnPool) Put(c net.Conn) {
	addr := strings.Split(c.LocalAddr().String(), ":")[0]
	connP.Lock()
	pool, ok := connP.pools[addr]
	connP.Unlock()
	if !ok {
		c.Close()
		return
	}
	pool.Put(c)
	return
}
