package sst

import (
	"errors"
	"fmt"

	"github.com/ttn-nguyen42/go-mini-lsm/internal/block"
	"github.com/ttn-nguyen42/go-mini-lsm/internal/types"
)

type iter struct {
	table    *SortedTable
	blkIter  types.SeekableIterator
	blkIndex int
}

func newIter(table *SortedTable) types.SeekableIterator {
	first, ok, err := table.Block(0)
	if err != nil {
		if errors.Is(err, block.ErrBlockEmpty) {
			first = nil
		}
		panic("unexpected error: " + err.Error())
	} else {
		if !ok {
			first = nil
		}
	}

	it := &iter{
		table:    table,
		blkIndex: 0,
	}
	if first != nil {
		it.blkIter = first.Scan()
	}
	return it
}

func (i *iter) HasNext() bool {
	return i.blkIter != nil && i.blkIter.HasNext()
}

func (i *iter) Key() types.Bytes {
	return i.blkIter.Key()
}

func (i *iter) Next() error {
	err := i.blkIter.Next()
	if err != nil {
		if !errors.Is(err, types.ErrIterEnd) {
			return err
		}
		i.blkIndex += 1
		blk, ok, err := i.table.Block(i.blkIndex)
		if err != nil {
			return fmt.Errorf("failed to get next block: %s", err)
		}
		if !ok {
			i.blkIter = nil
			return types.ErrIterEnd
		}
		i.blkIter = blk.Scan()
	}

	return nil
}

func (i *iter) Value() types.Bytes {
	return i.blkIter.Value()
}

func (i *iter) Seek(idx int) error {
	blk, _, err := i.table.Block(idx)
	if err != nil {
		return err
	}
	blkIter := blk.Scan()

	i.blkIndex = idx
	i.blkIter = blkIter
	return nil

}

func (i *iter) SeekToKey(key types.Bytes) error {
	for idx, meta := range i.table.blocks {
		if types.BytesComparator(key, meta.FirstKey) < 0 {
			continue
		}
		err := i.Seek(idx)
		if err != nil {
			return err
		}
		err = i.blkIter.SeekToKey(key)
		if err != nil {
			return err
		}
		break
	}

	return nil
}
