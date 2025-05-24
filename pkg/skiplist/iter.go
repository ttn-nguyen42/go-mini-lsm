package skiplist

import (
	"fmt"
)

var ErrIterEnded = fmt.Errorf("iterator ended")

func (s *skipListImpl[K, V]) Scan() Iterator[K, V] {
	return newIter(s)
}

type Iterator[K, V any] interface {
	HasNext() bool
	Next() error
	Key() K
	Value() V
	Close()
}

type listIter[K, V any] struct {
	list *skipListImpl[K, V]
	cur  *node[K, V]
	done bool
}

func newIter[K, V any](list *skipListImpl[K, V]) Iterator[K, V] {
	list.lock.RLock()

	first := list.head.getCell(0).next
	return &listIter[K, V]{
		list: list,
		cur:  first,
	}
}

func (l *listIter[K, V]) HasNext() bool {
	if l.done {
		return false
	}
	return l.cur != nil && l.cur != l.list.tail
}

func (l *listIter[K, V]) Key() K {
	if l.done || l.cur == nil || l.cur == l.list.tail {
		panic("iterator has ended")
	}
	return l.cur.key
}

func (l *listIter[K, V]) Next() error {
	if l.done {
		return ErrIterEnded
	}
	
	if l.cur == nil || l.cur == l.list.tail {
		l.done = true
		l.list.lock.RUnlock()
		return ErrIterEnded
	}

	l.cur = l.cur.getCell(0).next
	return nil
}

func (l *listIter[K, V]) Value() V {
	if l.done || l.cur == nil || l.cur == l.list.tail {
		panic("iterator has ended")
	}
	
	return l.cur.value
}

func (l *listIter[K, V]) Close() {
	if !l.done {
		l.done = true
		l.list.lock.RUnlock()
	}
}
