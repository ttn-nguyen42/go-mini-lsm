package memtable

import (
	"errors"
	"fmt"

	"github.com/ttn-nguyen42/go-mini-lsm/internal/types"
	"github.com/ttn-nguyen42/go-mini-lsm/pkg/skiplist"
)

var ErrIterEnded error = fmt.Errorf("iterator reached the end")

type iter struct {
	m  *memTable
	it skiplist.Iterator[types.Bytes, types.Bytes]
}

func newRangedIter(m *memTable, lower types.Bound[types.Bytes], upper types.Bound[types.Bytes]) types.ClosableIterator {
	return &iter{
		m:  m,
		it: m.list.Scan(lower, upper),
	}
}

func newIter(m *memTable) types.ClosableIterator {
	return &iter{
		m:  m,
		it: m.list.Iter(),
	}
}

func (i *iter) HasNext() bool {
	return i.it.HasNext()
}

func (i *iter) Key() types.Bytes {
	return i.it.Key()
}

func (i *iter) Next() error {
	if err := i.it.Next(); err != nil {
		if errors.Is(err, skiplist.ErrIterEnded) {
			return ErrIterEnded
		}

		return err
	}

	return nil
}

func (i *iter) Value() types.Bytes {
	return i.it.Value()
}

func (i *iter) Close() {
	i.it.Close()
}
