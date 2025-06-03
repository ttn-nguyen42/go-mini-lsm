package sst

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc32"

	"github.com/bits-and-blooms/bloom/v3"
)

// +-----------+-----------------+------------+-----------------+-------------------------+-------------------+--------------+--------------------+----------------+-----------------+
// | block #0  |  checksum (4b)  |  block #1  |  checksum (4b)  |  # of met. blocks (4b)  |  metadata blocks  |  CRC32 (4b)  |  met. offset (4b)  |  bloom filter  |  bf offset (4b) |
// +-----------+-----------------+------------+-----------------+-------------------------+-------------------+--------------+--------------------+----------------+-----------------+
func Decode(id uint32, f *FileObject) (*SortedTable, error) {
	t, err := decodeTable(f)
	t.id = id
	return t, err
}

func decodeTable(f *FileObject) (*SortedTable, error) {
	size := f.Size()
	buf := make([]byte, size)

	// Read bloom filter offset (last 4 bytes)
	_, err := f.ReadAt(buf[size-4:], int64(size-4))
	if err != nil {
		return nil, fmt.Errorf("failed to read bloom filter offset from file: %s", err)
	}
	blOffset := binary.BigEndian.Uint32(buf[size-4:])

	// Read bloom filter
	_, err = f.ReadAt(buf[blOffset:size-4], int64(blOffset))
	if err != nil {
		return nil, fmt.Errorf("failed to read bloom filter from file: %s", err)
	}
	bf := &bloom.BloomFilter{}
	if _, err = bf.ReadFrom(bytes.NewBuffer(buf[blOffset : size-4])); err != nil {
		return nil, fmt.Errorf("failed to read bloom filter: %s", err)
	}

	// Read metadata blocks offset (4 bytes before bloom filter)
	_, err = f.ReadAt(buf[blOffset-4:blOffset], int64(blOffset-4))
	if err != nil {
		return nil, fmt.Errorf("failed to read metadata blocks offset: %s", err)
	}
	metOffset := binary.BigEndian.Uint32(buf[blOffset-4 : blOffset])

	// Read metadata blocks
	_, err = f.ReadAt(buf[metOffset:blOffset-4], int64(metOffset))
	if err != nil {
		return nil, fmt.Errorf("failed to read metadata blocks: %s", err)
	}
	metadata, err := decodeBlockMetadatas(buf[metOffset : blOffset-4])
	if err != nil {
		return nil, err
	}

	// Read data checksum (4 bytes before metadata blocks)
	rawDataChecksum := buf[metOffset-4 : metOffset]
	_, err = f.ReadAt(rawDataChecksum, int64(metOffset-4))
	if err != nil {
		return nil, fmt.Errorf("failed to read data checksum: %s", err)
	}
	dataChecksum := binary.BigEndian.Uint32(rawDataChecksum)

	// Read data blocks
	data := buf[:metOffset-4]
	_, err = f.ReadAt(data, 0)
	if err != nil {
		return nil, fmt.Errorf("failed to read data block: %s", err)
	}

	// Verify checksum
	calcChecksum := crc32.ChecksumIEEE(data)
	if calcChecksum != dataChecksum {
		return nil, fmt.Errorf("data checksum mismatch")
	}

	// Construct SortedTable
	fm := metadata[0]
	lm := metadata[len(metadata)-1]

	return &SortedTable{
		file:            f,
		firstKey:        fm.FirstKey,
		lastKey:         lm.LastKey,
		filter:          bf,
		blocks:          metadata,
		blockMetaOffset: int(metOffset - 4),
	}, nil
}
