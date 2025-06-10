package memtable

import (
	"sync/atomic"

	"github.com/ttn-nguyen42/go-mini-lsm/internal/types"
	"github.com/ttn-nguyen42/go-mini-lsm/pkg/skiplist"
)

type MemTable interface {
	Put(key, value types.Bytes)
	Get(key types.Bytes) (types.Bytes, bool)
	Size() int
	Scan(l types.Bound[types.Bytes], r types.Bound[types.Bytes]) types.ClosableIterator
	Iter() types.ClosableIterator
	Id() int
}

type memTable struct {
	id   int
	list skiplist.SkipList[types.Bytes, types.Bytes]
	size atomic.Int32
}

func New(id int) MemTable {
	return &memTable{
		id:   id,
		list: newSkipList(),
		size: atomic.Int32{},
	}
}

func newSkipList() skiplist.SkipList[types.Bytes, types.Bytes] {
	res, _ := skiplist.New[types.Bytes, types.Bytes](types.BytesComparator, skiplist.WithMaxLevel(20))

	return res
}

func (m *memTable) Get(key types.Bytes) (types.Bytes, bool) {
	return m.list.Get(key)
}

func (m *memTable) Put(key types.Bytes, value types.Bytes) {
	estSize := len(key) + len(value)
	m.list.Put(key, value)

	m.size.Add(int32(estSize))
}

func (m *memTable) Size() int {
	return int(m.size.Load())
}

func (m *memTable) Scan(lower types.Bound[types.Bytes], upper types.Bound[types.Bytes]) types.ClosableIterator {
	return newRangedIter(m, lower, upper)
}

func (m *memTable) Id() int {
	return m.id
}

func (m *memTable) Iter() types.ClosableIterator {
	return newIter(m)
}
