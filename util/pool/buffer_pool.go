package pool

import (
	"fmt"
	"github.com/tiglabs/baudstorage/proto"
	"github.com/tiglabs/baudstorage/storage"
	"sync"
	"time"
)

type BufferPool struct {
	sync.Mutex
	pools map[int]*ChannelPool
}

func NewBufferPool() (bufferP *BufferPool) {
	return &BufferPool{pools: make(map[int]*ChannelPool)}
}

func (bufferP *BufferPool) Get(size int) ([]byte, error) {
	if !(size == storage.BlockSize || size == proto.HeaderSize) {
		return nil, fmt.Errorf("bufferPool can only support 65536 or 45 bytes")
	}

	bufferP.Lock()
	pool, ok := bufferP.pools[size]
	var (
		err error
		obj interface{}
	)
	bufferP.Unlock()
	factoryFunc := func(size interface{}) (interface{}, error) {
		data := make([]byte, size.(int))
		return data, nil
	}
	closeFunc := func(v interface{}) error { return nil }
	if !ok {
		poolConfig := &PoolConfig{
			InitialCap:  1024,
			MaxCap:      20480,
			Factory:     factoryFunc,
			Close:       closeFunc,
			IdleTimeout: time.Minute * 30,
			Para:        size,
		}
		pool, err = NewChannelPool(poolConfig)
		if err != nil {
			data, err := factoryFunc(size)
			return data.([]byte), err
		}
		bufferP.Lock()
		bufferP.pools[size] = pool
		bufferP.Unlock()
	}

	if obj, err = pool.Get(); err != nil {
		data, err := factoryFunc(size)
		return data.([]byte), err
	}
	data := obj.([]byte)

	return data, err
}

func (bufferP *BufferPool) Put(data []byte) {
	size := len(data)
	bufferP.Lock()
	pool, ok := bufferP.pools[size]
	bufferP.Unlock()
	if !ok {
		return
	}
	pool.Put(data)
	return
}
