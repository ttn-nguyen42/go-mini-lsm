package cache

import (
	"sync"
	"time"
)

// SizeableKey: comparable + sizeable
// Only allow string or []byte for K and V for now (Go doesn't allow interface in union)
type SizeableKey interface {
	comparable
	~string | ~[]byte
}

type Cache[K SizeableKey, V SizeableKey] interface {
	Set(key K, value V) (old V, replaced bool)
	Get(key K) (value V, ok bool)
	GetOrSet(key K, defaultVal V) (value V, isSet bool)
	Delete(key K) (ok bool)
	Contains(key K) (ok bool)
	Clear()
	Len() int
	Cap() int
	SizeBytes() int
}

type lruItem[K comparable, V any] struct {
	key  K
	val  V
	prev *lruItem[K, V] // more recently used
	next *lruItem[K, V] // less recently used
}

type lruCache[K SizeableKey, V SizeableKey] struct {
	lock sync.RWMutex

	head      *lruItem[K, V] // most recently used
	tail      *lruItem[K, V] // least recently used
	data      map[K]*lruItem[K, V]
	cap       int
	numItems  int // number of items
	sizeBytes int // total size in bytes of all keys and values

	opts *Options
}

func New[K SizeableKey, V SizeableKey](size int, options ...Option) Cache[K, V] {
	opts := newOptions(options...)

	return newLruCache[K, V](size, opts)
}

type eviction[K comparable, V any] struct {
	key K
	val V
	ts  int64
}

func newLruCache[K SizeableKey, V SizeableKey](size int, opts *Options) *lruCache[K, V] {
	head := &lruItem[K, V]{}
	tail := &lruItem[K, V]{}
	head.next = tail
	tail.prev = head

	return &lruCache[K, V]{
		lock:      sync.RWMutex{},
		cap:       size,
		numItems:  0,
		sizeBytes: 0,
		head:      head,
		tail:      tail,
		data:      make(map[K]*lruItem[K, V]),
		opts:      opts,
	}
}

func (l *lruCache[K, V]) Clear() {
	l.lock.Lock()
	defer l.lock.Unlock()
	l.clear()
}

func (l *lruCache[K, V]) clear() {
	l.numItems = 0
	l.sizeBytes = 0
	l.data = make(map[K]*lruItem[K, V])
	l.head.next = l.tail
	l.tail.prev = l.head
}

func (l *lruCache[K, V]) Contains(key K) (ok bool) {
	l.lock.RLock()
	defer l.lock.RUnlock()

	_, ok = l.data[key]
	return
}

func (l *lruCache[K, V]) Delete(key K) (ok bool) {
	l.lock.Lock()
	defer l.lock.Unlock()
	return l.delete(key)
}

func (l *lruCache[K, V]) delete(key K) (ok bool) {
	item, ok := l.data[key]
	if ok {
		l.pop(item)
		delete(l.data, key)
		l.numItems -= 1
		l.sizeBytes -= defaultLen(key) + defaultLen(item.val)
		return true
	}
	return false
}

func (l *lruCache[K, V]) Get(key K) (value V, ok bool) {
	l.lock.Lock()
	defer l.lock.Unlock()

	return l.get(key)
}

func (l *lruCache[K, V]) get(key K) (value V, ok bool) {
	item, ok := l.data[key]
	if !ok {
		return value, false
	}

	l.pop(item)
	l.pushRecent(item)
	return item.val, true
}

func (l *lruCache[K, V]) GetOrSet(key K, defaultVal V) (value V, isSet bool) {
	l.lock.Lock()
	defer l.lock.Unlock()

	item, ok := l.get(key)
	if !ok {
		l.setEvicted(key, defaultVal)
		return defaultVal, true
	}
	return item, false
}

func (l *lruCache[K, V]) Set(key K, value V) (old V, replaced bool) {
	l.lock.Lock()
	defer l.lock.Unlock()
	old, replaced, _ = l.setEvicted(key, value)
	return
}

func (l *lruCache[K, V]) pushRecent(i *lruItem[K, V]) {
	mostRecent := l.head.next
	mostRecent.prev = i
	i.prev = l.head
	i.next = mostRecent
	l.head.next = i
}

func (l *lruCache[K, V]) evictLeastRecent() *eviction[K, V] {
	tail := l.tail
	least := tail.prev
	moreRecent := least.prev
	moreRecent.next = tail
	tail.prev = moreRecent
	l.numItems -= 1
	l.sizeBytes -= defaultLen(least.key) + defaultLen(least.val)

	if l.opts != nil && l.opts.evictHook != nil {
		l.opts.evictHook(least.key, least.val, time.Now().UnixMilli())
	}

	return &eviction[K, V]{key: least.key, val: least.val, ts: time.Now().UnixMilli()}
}

func (l *lruCache[K, V]) setEvicted(key K, value V) (old V, replace bool, e *eviction[K, V]) {
	val, ok := l.data[key]
	if !ok {
		if l.shouldEvict() {
			e = l.evictLeastRecent()
			delete(l.data, e.key)
		}
		val = &lruItem[K, V]{key: key, val: value}
		l.pushRecent(val)
		l.data[key] = val
		l.numItems += 1
		l.sizeBytes += defaultLen(key) + defaultLen(value)
		var old V
		return old, false, e
	}
	old = val.val
	l.sizeBytes -= defaultLen(val.key) + defaultLen(val.val)
	val.val = value
	l.sizeBytes += defaultLen(val.key) + defaultLen(val.val)
	l.makeRecent(val)
	return old, true, nil
}

func (l *lruCache[K, V]) shouldEvict() bool {
	return l.numItems >= l.cap
}

func (l *lruCache[K, V]) isMostRecent(h *lruItem[K, V]) bool {
	return l.head.next == h
}

func (l *lruCache[K, V]) pop(h *lruItem[K, V]) {
	moreRecent := h.prev
	lessRecent := h.next

	moreRecent.next = lessRecent
	lessRecent.prev = moreRecent
}

func (l *lruCache[K, V]) makeRecent(h *lruItem[K, V]) bool {
	isRecent := l.isMostRecent(h)
	if !isRecent {
		l.pop(h)
		l.pushRecent(h)
	}
	return isRecent
}

func (l *lruCache[K, V]) Len() int {
	l.lock.RLock()
	defer l.lock.RUnlock()
	return l.numItems
}

func (l *lruCache[K, V]) SizeBytes() int {
	l.lock.RLock()
	defer l.lock.RUnlock()
	return l.sizeBytes
}

func (l *lruCache[K, V]) Cap() int {
	l.lock.RLock()
	defer l.lock.RUnlock()

	return l.cap
}

// Helper to get length of K or V
func defaultLen[T SizeableKey](v T) int {
	return len(v)
}
