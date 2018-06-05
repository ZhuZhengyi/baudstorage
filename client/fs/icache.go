package fs

import (
	"container/list"
	"sync"
	"time"

	"github.com/tiglabs/baudstorage/util/log"
)

type InodeCache struct {
	sync.RWMutex
	cache       map[uint64]*list.Element
	lruList     *list.List
	expiration  time.Duration
	maxElements int
}

func NewInodeCache(exp time.Duration, maxElements int) *InodeCache {
	return &InodeCache{
		cache:       make(map[uint64]*list.Element),
		lruList:     list.New(),
		expiration:  exp,
		maxElements: maxElements,
	}
}

func (ic *InodeCache) Put(inode *Inode) {
	ic.Lock()
	defer ic.Unlock()

	inode.setExpiration(ic.expiration)
	element := ic.lruList.PushFront(inode)
	log.LogDebugf("InodeCache Put: inode(%v)", inode)

	old, ok := ic.cache[inode.ino]
	if ok {
		ic.lruList.Remove(old)
		delete(ic.cache, inode.ino)
	}

	if ic.lruList.Len() >= ic.maxElements {
		ic.evict()
	}

	ic.cache[inode.ino] = element
}

func (ic *InodeCache) Get(ino uint64) *Inode {
	ic.Lock()
	defer ic.Unlock()

	element, ok := ic.cache[ino]
	if !ok {
		return nil
	}

	inode := element.Value.(*Inode)
	log.LogDebugf("InodeCache Get: now(%v) inode(%v)", time.Now().Format(LogTimeFormat), inode)
	if inode.expired() {
		ic.lruList.Remove(element)
		delete(ic.cache, ino)
		return nil
	}

	ic.lruList.MoveToFront(element)
	return inode
}

func (ic *InodeCache) Delete(ino uint64) {
	ic.Lock()
	defer ic.Unlock()
	log.LogDebugf("InodeCache Delete: ino(%v)", ino)
	element, ok := ic.cache[ino]
	if ok {
		ic.lruList.Remove(element)
		delete(ic.cache, ino)
	}
}

// Evict the oldest element if LRU len exceeds the limit.
// Under the protect of InodeCache lock.
func (ic *InodeCache) evict() {
	element := ic.lruList.Back()
	if element != nil {
		inode := ic.lruList.Remove(element).(*Inode)
		delete(ic.cache, inode.ino)
	}
}
