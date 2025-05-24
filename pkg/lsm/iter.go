package lsm

import (
	"fmt"

	"github.com/ttn-nguyen42/go-mini-lsm/internal/memtable"
	"github.com/ttn-nguyen42/go-mini-lsm/internal/types"
)

var ErrIterEnded error = fmt.Errorf("iterator has reached the end")

type Iterator interface {
	Key() types.Bytes
	Value() types.Bytes
	Next() error
	HasNext() bool
	Close()
}

type lsmIter struct {
	mergeIter *MergeIter
	done      bool
}

func NewIter(tables []memtable.MemTable) Iterator {
	mergeIter := NewMergeIter(tables)

	lsmIter := &lsmIter{
		mergeIter: mergeIter,
		done:      !mergeIter.HasNext(),
	}
	lsmIter.skipToNonDeleted()

	return lsmIter
}

func (l *lsmIter) Close() {
	l.mergeIter.Close()
}

func (l *lsmIter) HasNext() bool {
	return l.mergeIter.HasNext()
}

func (l *lsmIter) Key() types.Bytes {
	return l.mergeIter.Key()
}

func (l *lsmIter) Value() types.Bytes {
	return l.mergeIter.Value()
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
	l.mergeIter.Next()

	if !l.mergeIter.HasNext() {
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
