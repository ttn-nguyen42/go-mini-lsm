package lsm

import (
	"fmt"

	"github.com/ttn-nguyen42/go-mini-lsm/internal/memtable"
	"github.com/ttn-nguyen42/go-mini-lsm/internal/sst"
	"github.com/ttn-nguyen42/go-mini-lsm/internal/types"
)

var ErrIterEnded error = fmt.Errorf("iterator has reached the end")

type lsmIter struct {
	memTableIters  types.ClosableIterator
	l0SsTableIters types.Iterator
	done           bool
	upper          types.Bound[types.Bytes]
	lower          types.Bound[types.Bytes]
}

func NewIter(tables []memtable.MemTable, ssTables []sst.SortedTable, lower types.Bound[types.Bytes], upper types.Bound[types.Bytes]) types.ClosableIterator {

	lsmIter := &lsmIter{
		memTableIters:  nil,
		l0SsTableIters: nil,
		done:           true,
		lower:          lower,
		upper:          upper,
	}
	lsmIter.skipToNonDeleted()

	return lsmIter
}

func (l *lsmIter) Close() {
	l.memTableIters.Close()
}

func (l *lsmIter) HasNext() bool {
	return l.memTableIters.HasNext()
}

func (l *lsmIter) Key() types.Bytes {
	return l.memTableIters.Key()
}

func (l *lsmIter) Value() types.Bytes {
	return l.memTableIters.Value()
}

func (l *lsmIter) Next() error {
	if err := l.next(); err != nil {
		return err
	}

	if err := l.skipToNonDeleted(); err != nil {
		return err
	}

	return nil
}

func (l *lsmIter) next() error {
	l.memTableIters.Next()

	if !l.memTableIters.HasNext() {
		l.done = true
		return ErrIterEnded
	}

	return nil
}

func (l *lsmIter) skipToNonDeleted() error {
	for l.HasNext() && l.Value().Size() == 0 {
		if err := l.next(); err != nil {
			return err
		}
	}

	return nil
}
