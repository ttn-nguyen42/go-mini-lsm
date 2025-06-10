package skiplist

import (
	"fmt"

	"github.com/ttn-nguyen42/go-mini-lsm/internal/types"
)

var ErrIterEnded = fmt.Errorf("iterator ended")

type Iterator[K, V any] interface {
	HasNext() bool
	Next() error
	Key() K
	Value() V
	Close()
}

type listIter[K, V any] struct {
	list  *skipListImpl[K, V]
	cur   *node[K, V]
	done  bool
	lower types.Bound[K]
	upper types.Bound[K]
	cmp   types.Comparator[K]
}

func newRangedIter[K, V any](list *skipListImpl[K, V], lower types.Bound[K], upper types.Bound[K]) Iterator[K, V] {
	list.lock.RLock()

	first := moveToClosest(list, lower, upper)

	return &listIter[K, V]{
		list:  list,
		cur:   first,
		lower: lower,
		upper: upper,
		cmp:   types.Comparator[K](list.cmp),
	}
}

func newIter[K, V any](list *skipListImpl[K, V]) Iterator[K, V] {
	list.lock.RLock()

	first := list.head.getCell(0).next
	last := list.tail.getCell(0).prev

	return &listIter[K, V]{
		list:  list,
		cur:   first,
		lower: types.Include(first.key),
		upper: types.Include(last.key),
		cmp:   types.Comparator[K](list.cmp),
	}
}

func moveToClosest[K, V any](list *skipListImpl[K, V], lower types.Bound[K], upper types.Bound[K]) *node[K, V] {
	iter := list.head
	for lvl := list.opts.MaxLevel; lvl >= 0; lvl -= 1 {
		iter = list.getClosestToBound(iter, lower, lvl)
	}

	return iter.getCell(0).next
}

func (l *listIter[K, V]) HasNext() bool {
	if l.done {
		return false
	}
	return l.cur != nil && l.cur != l.list.tail && types.IsWithinBoundary(l.lower, l.upper, l.cur.key, l.cmp)
}

func (l *listIter[K, V]) Key() K {
	if l.done || l.cur == nil || l.cur == l.list.tail || !types.IsWithinBoundary(l.lower, l.upper, l.cur.key, l.cmp) {
		panic("iterator has ended")
	}
	return l.cur.key
}

func (l *listIter[K, V]) Next() error {
	if l.done {
		return ErrIterEnded
	}

	if l.cur == nil || l.cur == l.list.tail || !types.IsWithinBoundary(l.lower, l.upper, l.cur.key, l.cmp) {
		l.done = true
		l.list.lock.RUnlock()
		return ErrIterEnded
	}

	l.cur = l.cur.getCell(0).next
	return nil
}

func (l *listIter[K, V]) Value() V {
	if l.done || l.cur == nil || l.cur == l.list.tail || !types.IsWithinBoundary(l.lower, l.upper, l.cur.key, l.cmp) {
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

func (s *skipListImpl[K, V]) Scan(lower types.Bound[K], upper types.Bound[K]) Iterator[K, V] {
	return newRangedIter(s, lower, upper)
}

func (s *skipListImpl[K, V]) Iter() Iterator[K, V] {
	return newIter(s)
}
