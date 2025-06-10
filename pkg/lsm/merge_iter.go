package lsm

import (
	"fmt"

	"github.com/ttn-nguyen42/go-mini-lsm/internal/memtable"
	"github.com/ttn-nguyen42/go-mini-lsm/internal/sst"
	"github.com/ttn-nguyen42/go-mini-lsm/internal/types"
)

type MergeIter = types.Iterator

func NewMergeMemTableIter(tables []memtable.MemTable, lower types.Bound[types.Bytes], upper types.Bound[types.Bytes]) types.Iterator {
	if len(tables) == 0 {
		return types.NewMergeIter()
	}

	iters := make([]types.Iterator, 0, len(tables))
	for _, tb := range tables {
		it := tb.Scan(lower, upper)
		if it.HasNext() {
			iters = append(iters, it)
		} else {
			it.Close()
		}
	}

	return types.NewMergeIter(iters...)
}

func NewMergeSstTableIter(ssTables []sst.SortedTable, lower types.Bound[types.Bytes], upper types.Bound[types.Bytes]) types.Iterator {
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

	return types.NewMergeIter(iters...)
}
