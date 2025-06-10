package block

import (
	"bytes"
	"errors"

	"github.com/ttn-nguyen42/go-mini-lsm/internal/types"
)

type iter struct {
	blk  *Block
	entr *entry
	idx  int
}

func newIter(blk *Block) types.SeekableIterator {
	first, err := blk.first()
	if err != nil {
		if errors.Is(err, ErrBlockEmpty) {
			first = nil
		} else {
			panic("error getting first block entry: " + err.Error())
		}
	}

	return &iter{
		blk:  blk,
		entr: first,
		idx:  0,
	}
}

func (i *iter) HasNext() bool {
	return i.entr != nil
}

func (i *iter) Key() types.Bytes {
	if i.entr == nil {
		return nil
	}

	return i.entr.key
}

func (i *iter) Value() types.Bytes {
	if i.entr == nil {
		return nil
	}

	return i.entr.value
}

func (i *iter) Next() error {
	i.idx += 1

	return i.Seek(i.idx)
}

func (i *iter) Seek(idx int) error {
	if idx >= len(i.blk.offsets) {
		i.entr = nil
		return types.ErrIterEnd
	}

	offset := i.blk.offsets[idx]
	if err := i.seekOffset(offset); err != nil {
		return err
	}

	i.idx = idx
	return nil
}

func (i *iter) seekOffset(offset uint16) error {
	e := entry{}
	if err := e.decode(i.blk.data[offset:]); err != nil {
		return err
	}

	i.entr = &e
	return nil
}

// SeekToKey binary searches the block to find the first >= key
func (i *iter) SeekToKey(key types.Bytes) error {
	return i.search(key)
}

func (i *iter) search(key types.Bytes) error {
	lowIdx := 0
	highIdx := len(i.blk.offsets)

	for lowIdx < highIdx {
		midIdx := lowIdx + (highIdx-lowIdx)/2
		if err := i.Seek(midIdx); err != nil {
			return err
		}

		if !i.HasNext() {
			// Defensive: this should not happen, but if it does, break to avoid panic
			break
		}

		switch bytes.Compare(i.entr.key, key) {
		case 0:
			return nil
		case 1:
			highIdx = midIdx
		case -1:
			lowIdx = midIdx + 1
		}
	}

	if lowIdx >= len(i.blk.offsets) {
		return i.Seek(len(i.blk.offsets) - 1)
	}

	return i.Seek(lowIdx)
}
