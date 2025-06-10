package lsm

import (
	"bytes"
	"log"

	"github.com/ttn-nguyen42/go-mini-lsm/internal/memtable"
	"github.com/ttn-nguyen42/go-mini-lsm/internal/types"
	"github.com/ttn-nguyen42/go-mini-lsm/pkg/heap"
)

type memTableIterWrapper struct {
	iter types.ClosableIterator
	id   int
}

var iterWrapperCmp heap.Comparator[*memTableIterWrapper] = func(a, b *memTableIterWrapper) int {
	cmp := bytes.Compare(a.iter.Key(), b.iter.Key())
	if cmp != 0 {
		return cmp
	}

	if a.id > b.id {
		return -1
	} else {
		return 1
	}
}

type MergeIter struct {
	orgIter []types.ClosableIterator
	iters   heap.Heap[*memTableIterWrapper]
	curr    *memTableIterWrapper
}

func NewMergeIter(tables []memtable.MemTable) *MergeIter {
	if len(tables) == 0 {
		return &MergeIter{
			iters: heap.MinHeap(iterWrapperCmp),
			curr:  nil,
		}
	}

	orgIter := make([]types.ClosableIterator, 0, len(tables))
	wp := make([]*memTableIterWrapper, 0, len(tables))
	for _, tb := range tables {
		it := tb.Scan(types.Bound[types.Bytes]{}, types.Bound[types.Bytes]{})

		if !it.HasNext() {
			it.Close()
			continue
		}

		wp = append(wp, &memTableIterWrapper{iter: it, id: tb.Id()})
		orgIter = append(orgIter, it)
	}

	hp := heap.MinHeapSeeded(iterWrapperCmp, wp)
	curr := hp.Pop()

	return &MergeIter{
		iters:   hp,
		curr:    curr,
		orgIter: orgIter,
	}
}

func (m *MergeIter) HasNext() bool {
	if m.curr == nil {
		return false
	}

	return m.curr.iter.HasNext()
}

func (m *MergeIter) Key() types.Bytes {
	if m.curr == nil {
		panic("iterator has ended")
	}

	return m.curr.iter.Key()
}

func (m *MergeIter) Value() types.Bytes {
	if m.curr == nil {
		panic("iterator has ended")
	}

	return m.curr.iter.Value()
}

func (m *MergeIter) Next() error {
	currTable := m.curr.iter

	for {
		nextTable := m.iters.Peek()
		if nextTable == nil {
			break
		}

		cmp := bytes.Compare(currTable.Key(), nextTable.iter.Key())

		if cmp > 0 {
			log.Fatalf("curr table key must be smaller than next table key: %s %s", currTable.Key(), nextTable.iter.Key())
		}

		// If duplicate key found, advance next iter
		// Current table iter will the advanced later
		// When this.Next() is called, the 2 keys are always aligned
		if cmp == 0 {
			if nextTable.iter.HasNext() {
				if err := nextTable.iter.Next(); err != nil {
					m.iters.Pop()
					return err
				}
			}

			if !nextTable.iter.HasNext() {
				m.iters.Pop()
			}
		} else {
			break
		}
	}

	currTable.Next()

	if !currTable.HasNext() {
		m.curr = m.iters.Pop()
		return nil
	}

	nextTable := m.iters.Peek()
	if nextTable != nil {
		cmp := iterWrapperCmp(m.curr, nextTable)

		if cmp > 0 {
			m.iters.Push(m.curr)
			m.curr = m.iters.Pop()
		}
	}

	return nil
}

func (m *MergeIter) Close() {
	if m.curr != nil {
		m.curr.iter.Close()
	}

	for _, it := range m.orgIter {
		it.Close()
	}
}
