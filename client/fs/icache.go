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
	//TODO: make expiration a config
	inode.expiration = time.Now().Add(ic.expiration).UnixNano()
	log.LogDebugf("InodeCache Put: inode(%v)", inode)
	element := ic.lruList.PushFront(inode)
	old, ok := ic.cache[inode.ino]
	if ok {
		delete(ic.cache, inode.ino)
		ic.lruList.Remove(old)
	}
	ic.evict()
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
	if time.Now().UnixNano() > inode.expiration {
		delete(ic.cache, ino)
		ic.lruList.Remove(element)
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
		delete(ic.cache, ino)
		ic.lruList.Remove(element)
	}
}

// Evict the oldest element if LRU len exceeds the limit.
// Under the protect of InodeCache lock.
func (ic *InodeCache) evict() {
	if ic.lruList.Len() >= ic.maxElements {
		old := ic.lruList.Back()
		if old != nil {
			inode := ic.lruList.Remove(old).(*Inode)
			delete(ic.cache, inode.ino)
		}
	}
}
