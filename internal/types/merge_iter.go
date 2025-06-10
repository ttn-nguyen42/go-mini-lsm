package types

import (
	"fmt"

	"github.com/ttn-nguyen42/go-mini-lsm/pkg/heap"
)

type heapWrapper struct {
	id   int
	iter Iterator
}

var compareHeapWrapper heap.Comparator[*heapWrapper] = func(a, b *heapWrapper) int {
	cmp := BytesComparator(a.iter.Key(), b.iter.Key())
	if cmp != 0 {
		return cmp
	}

	// lastest first
	if a.id < b.id {
		return -1
	} else {
		return 1
	}
}

type mergeIter struct {
	heap heap.Heap[*heapWrapper]
	cur  *heapWrapper
}

func NewMergeIter(iters ...Iterator) Iterator {
	if len(iters) == 0 {
		return &mergeIter{heap: heap.MinHeap(compareHeapWrapper), cur: nil}
	}

	// Only include iterators that have items
	validWrappers := make([]*heapWrapper, 0, len(iters))
	for idx, iter := range iters {
		if iter.HasNext() {
			validWrappers = append(validWrappers, &heapWrapper{
				id:   idx,
				iter: iter,
			})
		}
	}

	if len(validWrappers) == 0 {
		return &mergeIter{heap: heap.MinHeap(compareHeapWrapper), cur: nil}
	}

	h := heap.MinHeapSeeded(compareHeapWrapper, validWrappers)
	cur := h.Pop()
	return &mergeIter{heap: h, cur: cur}
}

func (m *mergeIter) HasNext() bool {
	if m.cur == nil {
		return false
	}

	return m.cur.iter.HasNext()
}

func (m *mergeIter) Key() Bytes {
	if m.cur == nil {
		panic("iterator has ended")
	}

	return m.cur.iter.Key()
}

func (m *mergeIter) Next() error {
	cur := m.cur

	for m.heap.Len() > 0 {
		top := m.heap.Peek()

		if BytesComparator(cur.iter.Key(), top.iter.Key()) == 0 {
			if err := top.iter.Next(); err != nil {
				m.heap.Pop()
				continue
			}

			if !top.iter.HasNext() {
				m.heap.Pop()
				continue
			}
		} else {
			break
		}
	}

	if err := cur.iter.Next(); err != nil {
		return err
	}

	if !cur.iter.HasNext() {
		if m.heap.Len() <= 0 {
			return ErrIterEnd
		}
		m.cur = m.heap.Pop()
		return nil
	}

	// Compare with heap top and swap if necessary
	if m.heap.Len() > 0 {
		top := m.heap.Peek()
		if compareHeapWrapper(m.cur, top) > 0 {
			fmt.Println("SWAP", m.cur.iter.Key(), top.iter.Key())
			m.heap.Push(m.cur)
			m.cur = m.heap.Pop()
		}
	}
	return nil
}

func (m *mergeIter) Value() Bytes {
	if m.cur == nil {
		panic("iterator has ended")
	}

	return m.cur.iter.Value()
}
