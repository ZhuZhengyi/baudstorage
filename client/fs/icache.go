package fs

import (
	"sync"
	"time"

	"github.com/tiglabs/baudstorage/util/log"
)

type InodeCache struct {
	sync.RWMutex
	cache      map[uint64]*Inode
	expiration time.Duration
}

func NewInodeCache(exp time.Duration) *InodeCache {
	return &InodeCache{
		cache:      make(map[uint64]*Inode),
		expiration: exp,
	}
}

func (ic *InodeCache) Put(inode *Inode) {
	ic.Lock()
	defer ic.Unlock()
	//TODO: make expiration a config
	inode.expiration = time.Now().Add(ic.expiration).UnixNano()
	log.LogDebugf("InodeCache Put: inode(%v)", inode)
	ic.cache[inode.ino] = inode
}

func (ic *InodeCache) Get(ino uint64) *Inode {
	ic.Lock()
	defer ic.Unlock()
	inode, ok := ic.cache[ino]
	if !ok {
		return nil
	}
	log.LogDebugf("InodeCache Get: now(%v) inode(%v)", time.Now().Format(LogTimeFormat), inode)
	if time.Now().UnixNano() > inode.expiration {
		delete(ic.cache, ino)
		return nil
	}
	return inode
}

func (ic *InodeCache) Delete(ino uint64) {
	ic.Lock()
	defer ic.Unlock()
	log.LogDebugf("InodeCache Delete: ino(%v)", ino)
	_, ok := ic.cache[ino]
	if ok {
		delete(ic.cache, ino)
	}
	return
}
