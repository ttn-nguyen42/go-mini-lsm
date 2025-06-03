package block

import (
	"encoding/binary"
	"fmt"

	"github.com/ttn-nguyen42/go-mini-lsm/internal/types"
)

func Encode(b *Block) (types.Bytes, error) {
	buf, err := encode(b)
	if err != nil {
		return nil, err
	}

	return types.Bytes(buf), nil
}

func Decode(b types.Bytes) (*Block, error) {
	return decode(b)
}

func encode(blk *Block) ([]byte, error) {
	buf := make([]byte, blk.size)
	copy(buf, blk.data)

	pairCount := len(blk.offsets)
	bufOffset := len(blk.data)

	for _, offset := range blk.offsets {
		binary.BigEndian.PutUint16(buf[bufOffset:], offset)
		bufOffset += 2
	}

	binary.BigEndian.PutUint16(buf[bufOffset:], uint16(pairCount))

	return types.Bytes(buf), nil
}

func decode(data []byte) (*Block, error) {
	size := len(data)
	if size < 2 {
		return nil, fmt.Errorf("data too short to contain pair count")
	}
	pairCount := binary.BigEndian.Uint16(data[size-2:])

	offsetsLen := int(pairCount) * 2
	if size < 2+offsetsLen {
		return nil, fmt.Errorf("data too short for offsets: need at least %d bytes, got %d", 2+offsetsLen, size)
	}

	dataEnd := size - 2 - offsetsLen
	if dataEnd < 0 {
		return nil, fmt.Errorf("dataEnd negative: corrupted data")
	}
	offsetsBytes := data[dataEnd : size-2]
	if len(offsetsBytes)%2 != 0 {
		return nil, fmt.Errorf("offsetsBytes length not even: corrupted data")
	}

	offsets := make([]uint16, 0, len(offsetsBytes)/2)
	for bufOffset := dataEnd; bufOffset < size-2; bufOffset += 2 {
		if bufOffset+2 > size-2 {
			return nil, fmt.Errorf("offsets out of bounds")
		}
		offsets = append(offsets, binary.BigEndian.Uint16(data[bufOffset:bufOffset+2]))
	}

	if dataEnd > size {
		return nil, fmt.Errorf("dataEnd beyond data size")
	}

	if dataEnd < 0 {
		return nil, fmt.Errorf("dataEnd negative")
	}

	if size != dataEnd+len(offsetsBytes)+2 {
		return nil, fmt.Errorf("data size mismatch: expected %d, got %d", dataEnd+len(offsetsBytes)+2, size)
	}

	return &Block{data: data[:dataEnd], offsets: offsets}, nil
}
