package concat

import (
	"fmt"

	"github.com/ttn-nguyen42/go-mini-lsm/internal/sst"
	"github.com/ttn-nguyen42/go-mini-lsm/internal/types"
)

type concatIter struct {
	ssTables []sst.SortedTable
	idx      int
	cur      types.Iterator
}

func NewConcatIter(ssTables []sst.SortedTable) types.SeekableIterator {
	iter := &concatIter{
		ssTables: ssTables,
		idx:      0,
		cur:      nil,
	}
	iter.nextTable()

	return iter
}

func (c *concatIter) HasNext() bool {
	return c.cur != nil && c.cur.HasNext()
}

func (c *concatIter) Key() types.Bytes {
	if c.cur == nil {
		panic("iterator ended")
	}
	return c.cur.Key()
}

func (c *concatIter) Next() error {
	if c.cur == nil {
		return types.ErrIterEnd
	}
	if c.cur.HasNext() {
		return c.cur.Next()
	} else {
		return c.nextTable()
	}
}

func (c *concatIter) nextTable() error {
	if c.idx >= len(c.ssTables) {
		return types.ErrIterEnd
	}

	table := c.ssTables[c.idx]
	var err error
	c.cur, err = table.Scan()
	if err != nil {
		return err
	}
	c.idx += 1
	return nil
}

func (c *concatIter) Value() types.Bytes {
	if c.cur == nil {
		panic("iterator ended")
	}
	return c.cur.Value()
}

func (c *concatIter) Seek(idx int) error {
	if idx >= len(c.ssTables) {
		return fmt.Errorf("unknown sstable at idx: %d", idx)
	}
	table := c.ssTables[idx]
	var err error
	c.cur, err = table.Scan()
	if err != nil {
		return err
	}
	c.idx = idx + 1
	return nil
}

func (c *concatIter) SeekToKey(key types.Bytes) error {
	i := 0
	for _, t := range c.ssTables {
		if types.BytesComparator(key, t.FirstKey()) >= 0 {
			break
		}
		i += 1
	}
	if i >= len(c.ssTables) {
		return fmt.Errorf("key not found")
	}
	table := c.ssTables[i]
	var err error
	c.cur, err = table.Scan()
	if err != nil {
		return err
	}
	c.idx = i
	return nil
}
