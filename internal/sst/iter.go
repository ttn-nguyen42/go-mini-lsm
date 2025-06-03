package sst

import (
	"errors"
	"fmt"

	"github.com/ttn-nguyen42/go-mini-lsm/internal/block"
	"github.com/ttn-nguyen42/go-mini-lsm/internal/types"
)

var ErrIterEnd error = fmt.Errorf("iterator ended")

type Iterator interface {
	Key() types.Bytes
	Value() types.Bytes
	HasNext() bool
	Next() error
}

type iter struct {
	table    *SortedTable
	blkIter  block.Iterator
	blkIndex int
}

func newIter(table *SortedTable) Iterator {
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
		if !errors.Is(err, block.ErrIterEnd) {
			return err
		}
		i.blkIndex += 1
		blk, ok, err := i.table.Block(i.blkIndex)
		if err != nil {
			return fmt.Errorf("failed to get next block: %s", err)
		}
		if !ok {
			i.blkIter = nil
			return ErrIterEnd
		}
		i.blkIter = blk.Scan()
	}

	return nil
}

func (i *iter) Value() types.Bytes {
	return i.blkIter.Value()
}
