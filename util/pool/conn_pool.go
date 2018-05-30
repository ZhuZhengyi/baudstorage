package pool

import (
	"net"
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
	factoryFunc := func(addr interface{}) (interface{}, error) {
		var connect *net.TCPConn
		conn, err := net.DialTimeout("tcp", addr.(string), time.Second)
		if err == nil {
			connect, _ = conn.(*net.TCPConn)
			connect.SetKeepAlive(true)
			connect.SetNoDelay(true)
		}

		return connect, err
	}
	closeFunc := func(v interface{}) error { return v.(net.Conn).Close() }
	if !ok {
		poolConfig := &PoolConfig{
			InitialCap:  5,
			MaxCap:      300,
			Factory:     factoryFunc,
			Close:       closeFunc,
			IdleTimeout: time.Minute * 30,
			Para:        targetAddr,
		}
		pool, err = NewChannelPool(poolConfig)
		if err != nil {
			conn, err := factoryFunc(targetAddr)
			return conn.(net.Conn), err
		}
		connP.Lock()
		connP.pools[targetAddr] = pool
		connP.Unlock()
	}

	if obj, err = pool.Get(); err != nil {
		conn, err := factoryFunc(targetAddr)
		return conn.(net.Conn), err
	}
	c = obj.(net.Conn)

	return
}

func (connP *ConnPool) Put(c net.Conn) {
	addr := c.RemoteAddr().String()
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
