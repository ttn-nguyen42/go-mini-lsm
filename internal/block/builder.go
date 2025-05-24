package block

import (
	"github.com/ttn-nguyen42/go-mini-lsm/internal/types"
)

type BlockBuilder struct {
	offsets   []uint16
	data      []byte
	blockSize uint32
}

func Builder(options ...BuilderOption) *BlockBuilder {
	opts := getBuilderOpts(options...)

	return &BlockBuilder{
		blockSize: opts.BlockSize,
		data:      make([]byte, 0, opts.BlockSize),
		offsets:   make([]uint16, 0),
	}
}

// Add adds a key-value pair into the builder. Return false when the block is full
func (b *BlockBuilder) Add(key types.Bytes, value types.Bytes) bool {
	etr := getEntry(key, value)

	// curSize + entry size + uint16(offset)
	if b.curSize()+etr.size()+2 > int(b.blockSize) {
		return false
	}

	b.offsets = append(b.offsets, uint16(len(b.data)))
	b.data = append(b.data, etr.encode()...)

	return true
}

func (b BlockBuilder) curSize() int {
	// uint16(# of entries) + uint16(offsets) * (# of entries) + data
	return 2 + len(b.offsets)*2 + len(b.data)
}

func (b BlockBuilder) IsEmpty() bool {
	return len(b.offsets) == 0
}

func (b BlockBuilder) Build() Block {
	blk := Block{
		data:    b.data,
		offsets: b.offsets,
		size:    b.curSize(),
	}

	return blk
}

type entry struct {
	key      []byte
	value    []byte
	keyLen   uint16
	valueLen uint16
}

func (e entry) size() int {
	return int(2 + e.keyLen + 2 + e.valueLen)
}
