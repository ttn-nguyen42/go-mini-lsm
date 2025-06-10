package skiplist

import (
	"fmt"
	"math/rand"
	"strings"
	"sync"

	"github.com/ttn-nguyen42/go-mini-lsm/internal/types"
)

// SkipList is a generic interface for a skip list data structure.
type SkipList[K, V any] interface {
	IsEmpty() bool
	Get(key K) (V, bool)
	Delete(key K) bool
	Size() int
	Put(key K, value V)
	Scan(lower types.Bound[K], upper types.Bound[K]) Iterator[K, V]
	Iter() Iterator[K, V]
	String() string
}

type Comparator[K any] func(a, b K) int

type skipListImpl[K, V any] struct {
	opts *Options

	head    *node[K, V]
	tail    *node[K, V]
	lock    sync.RWMutex
	random  *rand.Rand
	curSize int
	cmp     Comparator[K]
}

func New[K, V any](cmp Comparator[K], options ...Option) (SkipList[K, V], error) {
	opts, err := getOptions(options...)
	if err != nil {
		return nil, err
	}

	impl := &skipListImpl[K, V]{
		opts:   opts,
		lock:   sync.RWMutex{},
		random: rand.New(rand.NewSource(rand.Int63())),
		cmp:    cmp,
	}

	impl.init()
	return impl, nil
}

func (s *skipListImpl[K, V]) init() {
	s.head = newNode[K, V](s.opts.MaxLevel)
	s.tail = newNode[K, V](s.opts.MaxLevel)

	for i := 0; i <= s.opts.MaxLevel; i += 1 {
		s.head.cells = append(s.head.cells, newCell(nil, s.tail))
		s.tail.cells = append(s.tail.cells, newCell(s.head, nil))
	}

	s.curSize = 0
}

func (s *skipListImpl[K, V]) IsEmpty() bool {
	s.lock.RLock()
	defer s.lock.RUnlock()

	return s.isEmpty()
}

func (s *skipListImpl[K, V]) Size() int {
	s.lock.RLock()
	defer s.lock.RUnlock()

	return s.curSize
}

func (s *skipListImpl[K, V]) Get(key K) (V, bool) {
	s.lock.RLock()
	defer s.lock.RUnlock()

	return s.get(key)
}

func (s *skipListImpl[K, V]) Delete(key K) bool {
	s.lock.Lock()
	defer s.lock.Unlock()

	return s.delete(key)
}

func (s *skipListImpl[K, V]) Put(key K, value V) {
	s.lock.Lock()
	defer s.lock.Unlock()

	s.put(key, value)
}

func (s *skipListImpl[K, V]) isEmpty() bool {
	return s.curSize == 0
}

func (s *skipListImpl[K, V]) get(key K) (V, bool) {
	var empty V

	if s.isEmpty() {
		return empty, false
	}

	iter := s.head
	for lvl := s.opts.MaxLevel; lvl >= 0; lvl-- {
		iter = s.getClosest(iter, key, lvl)

		if s.cmp(iter.key, key) == 0 {
			return iter.value, true
		}
	}

	return empty, false
}

func (s *skipListImpl[K, V]) delete(key K) bool {
	if s.isEmpty() {
		return false
	}

	iter := s.head
	for lvl := s.opts.MaxLevel; lvl >= 0; lvl -= 1 {
		iter = s.getClosest(iter, key, lvl)

		if s.cmp(iter.key, key) == 0 {
			cell := iter.getCell(lvl)

			prev := cell.prev
			next := cell.next

			prevCell := prev.getCell(lvl)
			nextCell := next.getCell(lvl)

			prevCell.next = next
			nextCell.prev = prev

			s.curSize -= 1

			return true
		}
	}

	return false
}

func (s *skipListImpl[K, V]) put(key K, value V) {
	iter := s.head
	for lvl := s.opts.MaxLevel; lvl >= 0; lvl -= 1 {
		iter = s.getClosest(iter, key, lvl)
	}

	if s.cmp(iter.key, key) == 0 {
		iter.value = value
		return
	}

	n := newNode[K, V](s.opts.MaxLevel)
	n.key = key
	n.value = value
	for lvl := s.opts.MaxLevel; lvl >= 0; lvl -= 1 {
		shouldOnLevel := s.random.Int31()%2 != 0
		if shouldOnLevel {
			n.cells = append(n.cells, newCell[K, V](nil, nil))
		}
	}

	iter = s.head
	for lvl := s.opts.MaxLevel; lvl >= 0; lvl -= 1 {
		iter = s.getClosest(iter, key, lvl)

		if lvl < len(n.cells) {
			iterCell := iter.getCell(lvl)
			currCell := n.getCell(lvl)
			nextCell := iterCell.next.getCell(lvl)

			nextCell.prev = n
			currCell.next = iterCell.next

			currCell.prev = iter
			iterCell.next = n
		}
	}

	s.curSize += 1
}

func (s *skipListImpl[K, V]) getClosest(start *node[K, V], key K, level int) *node[K, V] {
	iter := start
	cell := iter.getCell(level)

	for cell != nil && cell.next != s.tail {
		if s.cmp(cell.next.key, key) > 0 {
			break
		}

		iter = cell.next
		cell = iter.getCell(level)
	}

	return iter
}

func (s *skipListImpl[K, V]) getClosestToBound(start *node[K, V], lower types.Bound[K], level int) *node[K, V] {
	iter := start
	cell := iter.getCell(level)
	cmp := types.Comparator[K](s.cmp)

	for cell != nil && cell.next != s.tail {
		if !lower.IsBefore(cell.next.key, cmp) {
			break
		}
		iter = cell.next
		cell = iter.getCell(level)
	}

	return iter
}

func (s *skipListImpl[K, V]) String() string {
	s.lock.RLock()
	defer s.lock.RUnlock()

	sb := strings.Builder{}

	for lvl := s.opts.MaxLevel; lvl >= 0; lvl -= 1 {
		sb.WriteString("Level ")
		sb.WriteString(fmt.Sprintf("%d", lvl))
		sb.WriteString(": ")

		sb.WriteString("HEAD -> ")

		iter := s.head.getCell(lvl).next
		for iter != nil && iter != s.tail {
			cell := iter.getCell(lvl)
			sb.WriteString(fmt.Sprintf("%v", iter.key))
			sb.WriteString(" -> ")

			iter = cell.next
		}

		sb.WriteString("TAIL \n")
	}

	return sb.String()
}
