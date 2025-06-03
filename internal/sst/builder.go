package sst

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"

	"github.com/bits-and-blooms/bloom/v3"

	"github.com/ttn-nguyen42/go-mini-lsm/internal/block"
	"github.com/ttn-nguyen42/go-mini-lsm/internal/types"
)

type Builder struct {
	blockBuilder *block.Builder
	data         []byte
	firstKey     types.Bytes
	lastKey      types.Bytes
	metas        []BlockMeta
	keys         []types.Bytes
	blockSize    uint32
}

func NewBuilder(blockSize uint32) *Builder {
	return &Builder{
		metas:        make([]BlockMeta, 0),
		firstKey:     nil,
		lastKey:      nil,
		keys:         make([]types.Bytes, 0),
		blockBuilder: block.NewBuilder(block.WithBlockSize(blockSize)),
		blockSize:    blockSize,
	}
}

// +-----------+-----------------+------------+-----------------+-------------------------+-------------------+--------------+--------------------+----------------+-----------------+
// | block #0  |  checksum (4b)  |  block #1  |  checksum (4b)  |  # of met. blocks (4b)  |  metadata blocks  |  CRC32 (4b)  |  met. offset (4b)  |  bloom filter  |  bf offset (4b) |
// +-----------+-----------------+------------+-----------------+-------------------------+-------------------+--------------+--------------------+----------------+-----------------+
func (b *Builder) Build(id uint32, filePath string) (*SortedTable, error) {
	if err := b.refreshBlock(); err != nil {
		return nil, fmt.Errorf("failed to refresh block: %s", err)
	}

	bl := b.getBloomFilter()
	blBin, err := bl.MarshalBinary() // bloom filter
	if err != nil {
		return nil, err
	}

	s := getSstSizeEstimate(len(b.data), b.metas, len(blBin))
	buf := make([]byte, s)

	off := 0
	copy(buf[:len(b.data)], b.data)
	off += len(b.data)

	dataChecksum := crc32.ChecksumIEEE(buf[:off])
	binary.BigEndian.PutUint32(buf[off:off+4], dataChecksum) // block data checksum
	off += 4
	

	blkMetaSize := estimateBlockMetadatas(b.metas)
	metaOff := off

	encodeBlockMetadatas(buf[off:off+blkMetaSize], b.metas) // block metadatas
	off += blkMetaSize

	binary.BigEndian.PutUint32(buf[off:off+4], uint32(metaOff)) // metadata offset
	off += 4

	blOff := off
	copy(buf[off:off+len(blBin)], blBin)
	off += len(blBin)

	binary.BigEndian.PutUint32(buf[off:off+4], uint32(blOff)) // bloom filter offset
	off += 4

	return b.flushSsTable(id, buf[:off], bl, filePath, metaOff)
}

func getSstSizeEstimate(dataSize int, blkMeta []BlockMeta, blfSize int) int {
	dataSize += 4                               // block data checksum
	dataSize += estimateBlockMetadatas(blkMeta) // block metadata
	dataSize += 4                               // metadata offset
	dataSize += blfSize                         // bloom filter
	dataSize += 4                               // bloom filter offset
	return dataSize
}

func (b *Builder) Add(key types.Bytes, value types.Bytes) error {
	if b.firstKey == nil {
		b.firstKey = key
	}

	if b.blockBuilder.Add(key, value) {
		b.lastKey = key
		b.keys = append(b.keys, key)
		return nil
	}

	if err := b.refreshBlock(); err != nil {
		return err
	}

	b.firstKey = key
	b.lastKey = key
	b.keys = append(b.keys, b.firstKey)

	return nil
}

func (b *Builder) refreshBlock() error {
	currBuilder := b.blockBuilder
	b.blockBuilder = block.NewBuilder(block.WithBlockSize(b.blockSize))

	blk := currBuilder.Build()

	blkData, err := block.Encode(&blk)
	if err != nil {
		return fmt.Errorf("failed to encode block data: %s", err)
	}

	blkMeta := BlockMeta{
		Offset:   uint32(len(b.data)),
		FirstKey: b.firstKey,
		LastKey:  b.lastKey,
	}

	checksum := crc32.ChecksumIEEE(blkData)

	b.metas = append(b.metas, blkMeta)
	b.data = append(b.data, blkData...)
	b.data = binary.BigEndian.AppendUint32(b.data, checksum)

	return nil
}

func (b *Builder) getBloomFilter() *bloom.BloomFilter {
	f := bloom.NewWithEstimates(uint(len(b.keys)), 0.01)
	for _, k := range b.keys {
		f.Add(k)
	}
	return f
}

func (b *Builder) flushSsTable(id uint32, buf []byte, bl *bloom.BloomFilter, filePath string, blockMetaOffset int) (*SortedTable, error) {
	headMeta := b.metas[0]
	tailMeta := b.metas[len(b.metas)-1]

	fo, err := Write(buf, filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to create file: %s", err)
	}

	return &SortedTable{
		id:              id,
		filter:          bl,
		firstKey:        headMeta.FirstKey,
		lastKey:         tailMeta.LastKey,
		blocks:          b.metas,
		file:            fo,
		blockMetaOffset: blockMetaOffset,
	}, nil
}
