package memtable

import (
	"errors"
	"fmt"

	"github.com/ttn-nguyen42/go-mini-lsm/internal/types"
	"github.com/ttn-nguyen42/go-mini-lsm/pkg/skiplist"
)

var ErrIterEnded error = fmt.Errorf("iterator reached the end")

type Iterator interface {
	HasNext() bool
	Next() error
	Key() types.Bytes
	Value() types.Bytes
	Close()
}

type mergeIter struct {
	m  *memTable
	it skiplist.Iterator[types.Bytes, types.Bytes]
}

func newIter(m *memTable) Iterator {
	return &mergeIter{
		m:  m,
		it: m.list.Scan(),
	}
}

func (i *mergeIter) HasNext() bool {
	return i.it.HasNext()
}

func (i *mergeIter) Key() types.Bytes {
	return i.it.Key()
}

func (i *mergeIter) Next() error {
	if err := i.it.Next(); err != nil {
		if errors.Is(err, skiplist.ErrIterEnded) {
			return ErrIterEnded
		}

		return err
	}

	return nil
}

func (i *mergeIter) Value() types.Bytes {
	return i.it.Value()
}

func (i *mergeIter) Close() {
	i.it.Close()
}
