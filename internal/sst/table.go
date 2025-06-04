package sst

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc32"

	"github.com/bits-and-blooms/bloom/v3"
	"github.com/ttn-nguyen42/go-mini-lsm/internal/block"
	"github.com/ttn-nguyen42/go-mini-lsm/internal/types"
)

var ErrClosed = fmt.Errorf("table closed")

type SortedTable struct {
	id              int32
	firstKey        types.Bytes
	lastKey         types.Bytes
	filter          *bloom.BloomFilter
	blocks          []BlockMeta
	file            *FileObject
	blockMetaOffset int

	cache BlockCache

	closed bool
}

func (s *SortedTable) Close() error {
	s.closed = true
	return s.file.Close()
}

func (s *SortedTable) File() *FileObject {
	return s.file
}

func (s *SortedTable) Id() int32 {
	return s.id
}

func (s *SortedTable) FirstKey() types.Bytes {
	return s.firstKey
}

func (s *SortedTable) LastKey() types.Bytes {
	return s.lastKey
}

func (s *SortedTable) Contains(key types.Bytes) bool {
	if bytes.Compare(key, s.firstKey) < 0 {
		return false
	}
	if bytes.Compare(key, s.lastKey) > 0 {
		return false
	}
	if bytes.Equal(key, s.firstKey) {
		return true
	}
	if bytes.Equal(key, s.lastKey) {
		return true
	}

	return s.filter.Test(key)
}

func (s *SortedTable) Block(idx int) (*block.Block, bool, error) {
	if s.closed {
		return nil, false, ErrClosed
	}

	if idx >= len(s.blocks) {
		return nil, false, nil
	}

	var err error
	key := CacheKey{SstId: s.id, BlockId: int32(idx)}
	blk, _ := s.cache.GetOrSet(key, func() (*block.Block, bool) {
		var blk *block.Block
		blk, err = s.readBlock(idx)
		if err != nil {
			panic("failed to read block: " + err.Error())
		}
		return blk, true
	})

	return blk, true, nil
}

func (s *SortedTable) readBlock(idx int) (*block.Block, error) {
	end := s.blockMetaOffset - 4 // without the data checksum
	if idx+1 < len(s.blocks) {
		end = int(s.blocks[idx+1].Offset)
	}

	start := int(s.blocks[idx].Offset)
	data := make([]byte, end-start)

	_, err := s.file.ReadAt(data, int64(start))
	if err != nil {
		return nil, fmt.Errorf("failed to read block idx=%d: %s", idx, err)
	}

	fileChecksum := binary.BigEndian.Uint32(data[len(data)-4:])

	data = data[:len(data)-4]

	calcChecksum := crc32.ChecksumIEEE(data)
	if calcChecksum != fileChecksum {
		return nil, fmt.Errorf("block checksum mismatch")
	}

	return block.Decode(data)
}

func (s *SortedTable) NumBlocks() int {
	return len(s.blocks)
}

func (s *SortedTable) IsClosed() bool {
	return s.closed
}

func (s *SortedTable) Scan() (Iterator, error) {
	if s.closed {
		return nil, ErrClosed
	}

	return newIter(s), nil
}
