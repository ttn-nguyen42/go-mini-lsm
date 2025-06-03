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
	metaOffset := len(b.data)

	b.data = encodeBlockMetadatas(b.data, b.metas)
	b.data = binary.BigEndian.AppendUint32(b.data, uint32(metaOffset))

	bl := b.getBloomFilter()
	blOffset := len(b.data)
	blBin, err := bl.MarshalBinary()
	if err != nil {
		return nil, fmt.Errorf("failed to serialize bloom filter: %s", err)
	}
	b.data = append(b.data, blBin...)
	b.data = binary.BigEndian.AppendUint32(b.data, uint32(blOffset))

	return b.flushSsTable(id, bl, filePath, metaOffset)
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
	b.keys = make([]types.Bytes, 0)

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
		Offset:   uint32(blkData.Size()),
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
	f := bloom.NewWithEstimates(uint(len(b.metas)), 0.01)
	for _, k := range b.keys {
		f.Add(k)
	}
	return f
}

func (b *Builder) flushSsTable(id uint32, bl *bloom.BloomFilter, filePath string, blockMetaOffset int) (*SortedTable, error) {
	headMeta := b.metas[0]
	tailMeta := b.metas[len(b.metas)-1]

	fo, err := Write(b.data, filePath)
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
