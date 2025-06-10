package lsm

import (
	"fmt"

	"github.com/ttn-nguyen42/go-mini-lsm/internal/memtable"
	"github.com/ttn-nguyen42/go-mini-lsm/internal/sst"
	"github.com/ttn-nguyen42/go-mini-lsm/internal/types"
	"github.com/ttn-nguyen42/go-mini-lsm/pkg/lsm/concat"
)

type MergeIter = types.Iterator

func SelectMemTableItersInRange(tables []memtable.MemTable, lower types.Bound[types.Bytes], upper types.Bound[types.Bytes]) []types.ClosableIterator {
	iters := make([]types.ClosableIterator, 0, len(tables))

	if len(tables) == 0 {
		return iters
	}

	for _, tb := range tables {
		it := tb.Scan(lower, upper)
		if it.HasNext() {
			iters = append(iters, it)
		} else {
			it.Close()
		}
	}

	return iters
}

func SelectSstItersInRange(ssTables []sst.SortedTable, lower types.Bound[types.Bytes], upper types.Bound[types.Bytes]) []types.Iterator {
	iters := make([]types.Iterator, 0, len(ssTables))

	for _, t := range ssTables {
		if t.OverlapKeyRange(lower, upper) {
			sstIter, err := t.Scan()
			if err != nil {
				panic(fmt.Sprintf("sorted table prematurely closed: %d", t.Id()))
			}
			if err := sstIter.SeekToKey(lower.Data()); err != nil {
				panic("expected sstable to include this key range, got err instead: " + err.Error())
			}
			if sstIter.HasNext() {
				iters = append(iters, sstIter)
			}
		}
	}

	return iters
}

func SelectLeveledSstInRange(ssTables [][]sst.SortedTable, lower types.Bound[types.Bytes], upper types.Bound[types.Bytes]) []types.Iterator {
	iters := make([]types.Iterator, 0, len(ssTables))
	for _, tables := range ssTables {
		levelIters := make([]sst.SortedTable, 0, len(tables))
		for _, table := range tables {
			if table.OverlapKeyRange(lower, upper) {
				levelIters = append(levelIters, table)
			}
		}
		cc := concat.NewConcatIter(levelIters)
		cc.SeekToKey(lower.Data())
		iters = append(iters, cc)
	}
	return iters
}
