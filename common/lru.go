package common

import (
	"container/list"
	"sync"
)

type entry[K comparable, V any] struct {
	key   K
	value *V
}

type LRUCache[K comparable, V any] struct {
	lru            *list.List
	cache          map[K]*list.Element
	capacity       uint32
	removeCallBack func(K, *V)
}

func NewLRUCache[K comparable, V any](cap uint32) *LRUCache[K, V] {
	return &LRUCache[K, V]{
		lru:            list.New(),
		capacity:       cap,
		cache:          make(map[K]*list.Element),
		removeCallBack: nil,
	}
}

func NewLRUCacheWithRemoveCallBack[K comparable, V any](cap uint32, callback func(K, *V)) *LRUCache[K, V] {
	return &LRUCache[K, V]{
		lru:            list.New(),
		capacity:       cap,
		cache:          make(map[K]*list.Element),
		removeCallBack: callback,
	}
}

// put
func (l *LRUCache[K, V]) Put(key K, value *V) {
	if e, ok := l.cache[key]; ok {
		l.lru.MoveToFront(e)
		e.Value.(*entry[K, V]).value = value
		return
	}
	if uint32(l.lru.Len()) >= l.capacity {
		oldest := l.lru.Back()
		l.lru.Remove(oldest)

		entry := oldest.Value.(*entry[K, V])
		delete(l.cache, entry.key)
		if l.removeCallBack != nil {
			go l.removeCallBack(entry.key, entry.value)
		}
	}
	l.cache[key] = l.lru.PushFront(&entry[K, V]{key: key, value: value})
}

// Get
func (l *LRUCache[K, V]) Get(key K) *V {
	if e, ok := l.cache[key]; ok {
		l.lru.MoveToFront(e)
		return e.Value.(*entry[K, V]).value
	}
	return nil
}

// Remove
func (l *LRUCache[K, V]) Remove(key K) {
	if e, ok := l.cache[key]; ok {
		l.lru.Remove(e)
		delete(l.cache, key)
		if l.removeCallBack != nil {
			entry := e.Value.(*entry[K, V])
			go l.removeCallBack(entry.key, entry.value)
		}
	}
}

// all value
func (l *LRUCache[K, V]) AllValue() []Tuple[K, *V] {
	var res []Tuple[K, *V]
	for e := l.lru.Front(); e != nil; e = e.Next() {
		res = append(res, Tuple[K, *V]{e.Value.(*entry[K, V]).key, e.Value.(*entry[K, V]).value})
	}
	return res
}

// Clear
func (l *LRUCache[K, V]) Clear() {
	l.cache = make(map[K]*list.Element)
	if l.removeCallBack != nil {
		for e := l.lru.Front(); e != nil; e = e.Next() {
			entry := e.Value.(*entry[K, V])
			go l.removeCallBack(entry.key, entry.value)
		}
	}
	l.lru = list.New()
}

// sync call back clear
func (l *LRUCache[K, V]) SyncClear() {
	l.cache = make(map[K]*list.Element)
	if l.removeCallBack != nil {
		wg := sync.WaitGroup{}
		wg.Add(l.lru.Len())
		for e := l.lru.Front(); e != nil; e = e.Next() {
			en := e.Value.(*entry[K, V])
			go func(en *entry[K, V]) {
				l.removeCallBack(en.key, en.value)
				wg.Done()
			}(en)
		}
		wg.Wait()
	}
	l.lru = list.New()
}
