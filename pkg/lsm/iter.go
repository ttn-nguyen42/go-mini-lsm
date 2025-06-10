package lsm

import (
	"github.com/ttn-nguyen42/go-mini-lsm/internal/memtable"
	"github.com/ttn-nguyen42/go-mini-lsm/internal/sst"
	"github.com/ttn-nguyen42/go-mini-lsm/internal/types"
)

type lsmIter struct {
	memTableIters  []types.ClosableIterator
	l0SsTableIters []types.Iterator
	leveledIters   []types.Iterator
	done           bool
	upper          types.Bound[types.Bytes]
	lower          types.Bound[types.Bytes]

	mergeIter types.Iterator
}

func NewIter(tables []memtable.MemTable, l0SsTables []sst.SortedTable, leveledSsTables [][]sst.SortedTable, lower types.Bound[types.Bytes], upper types.Bound[types.Bytes]) types.ClosableIterator {

	lsmIter := &lsmIter{
		memTableIters:  SelectMemTableItersInRange(tables, lower, upper),
		l0SsTableIters: SelectSstItersInRange(l0SsTables, lower, upper),
		leveledIters:   SelectLeveledSstInRange(leveledSsTables, lower, upper),
		done:           true,
		lower:          lower,
		upper:          upper,
	}
	lsmIter.initIters()
	lsmIter.skipToNonDeleted()

	return lsmIter
}

func (l *lsmIter) initIters() {
	asNormalIters := make([]types.Iterator, 0, len(l.memTableIters))
	for _, it := range l.memTableIters {
		asNormalIters = append(asNormalIters, it)
	}
	memTableIter := types.NewMergeIter(asNormalIters...)
	l0SstIter := types.NewMergeIter(l.l0SsTableIters...)
	leveledIter := types.NewMergeIter(l.leveledIters...)

	memTableL0Iter := types.NewTwoWayIter(memTableIter, l0SstIter)
	l.mergeIter = types.NewTwoWayIter(memTableL0Iter, leveledIter)
}

func (l *lsmIter) Close() {
	for _, it := range l.memTableIters {
		it.Close()
	}
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
		return types.ErrIterEnd
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
