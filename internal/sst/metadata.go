package sst

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"

	"github.com/ttn-nguyen42/go-mini-lsm/internal/types"
)

// +--------------+----------------------+-------------+---------------------+-----------+
// | offset (4b)  |  first key len (2b)  |  first key  |  last key len (2b)  |  last key |
// +--------------+----------------------+-------------+---------------------+-----------+
type BlockMeta struct {
	Offset   uint32
	FirstKey types.Bytes
	LastKey  types.Bytes
}

func (m *BlockMeta) Size() int {
	offsetSize := 4

	firstKeyLengthSize := 2
	firstKeySize := len(m.FirstKey)

	lastKeyLengthSize := 2
	lastKeySize := len(m.LastKey)

	return offsetSize + firstKeyLengthSize + firstKeySize + lastKeyLengthSize + lastKeySize
}

func (b *BlockMeta) Decode(rd io.Reader) (int, error) {
	rawOff := make([]byte, 4)
	total := 0
	
	n, err := rd.Read(rawOff)
	if err != nil {
		return total, err
	}
	total += n

	offset := binary.BigEndian.Uint32(rawOff)
	b.Offset = offset

	rawKeyLen := make([]byte, 2)
	
	n, err = rd.Read(rawKeyLen)
	if err != nil {
		return total, err
	}
	total += n

	firstKeyLen := binary.BigEndian.Uint16(rawKeyLen)
	b.FirstKey = make(types.Bytes, firstKeyLen)
	
	n, err = rd.Read(b.FirstKey)
	if err != nil {
		return total, err
	}
	total += n

	n, err = rd.Read(rawKeyLen)
	if err != nil {
		return total, err
	}
	total += n

	lastKeyLen := binary.BigEndian.Uint16(rawKeyLen)
	b.LastKey = make(types.Bytes, lastKeyLen)
	
	n, err = rd.Read(b.LastKey)
	if err != nil {
		return total, err
	}
	total += n

	return total, nil
}

func (b *BlockMeta) Encode(data []byte) int {
	off := 0

	binary.BigEndian.PutUint32(data[off:off+4], b.Offset)
	off += 4

	binary.BigEndian.PutUint16(data[off:off+2], uint16(len(b.FirstKey)))
	off += 2

	copy(data[off:off+len(b.FirstKey)], b.FirstKey)
	off += len(b.FirstKey)

	binary.BigEndian.PutUint16(data[off:off+2], uint16(len(b.LastKey)))
	off += 2

	copy(data[off:off+len(b.LastKey)], b.LastKey)
	off += len(b.LastKey)

	return off
}

func estimateBlockMetadatas(data []BlockMeta) int {
	total := 0
	total += 4 // number of metadata
	for _, m := range data {
		total += m.Size() // metadata
	}
	return total + 4 // checksum
}

// +-------------------+-------------------+----------------+
// | # of blocks (4b)  |  metadata blocks  |  CRC32 cs (4b) |
// +-------------------+-------------------+----------------+
func encodeBlockMetadatas(data []byte, metadata []BlockMeta) {
	s := 0

	binary.BigEndian.PutUint32(data[s:s+4], uint32(len(metadata))) // number of metadata blocks
	e := s + 4
	for _, m := range metadata {
		written := m.Encode(data[e : e+m.Size()])
		e += written
	} // metadata

	h := crc32.ChecksumIEEE(data[s+4 : e])
	binary.BigEndian.PutUint32(data[e:e+4], h) // checksum
}

func decodeBlockMetadatas(data []byte) ([]BlockMeta, error) {
	rawNum := data[:4]
	n := binary.BigEndian.Uint32(rawNum)
	metadata := make([]BlockMeta, n)

	blocks := data[4 : len(data)-4]
	calculatedChecksum := crc32.ChecksumIEEE(blocks)
	dataBuf := bytes.NewBuffer(blocks)

	cs := data[len(data)-4:]

	fileChecksum := binary.BigEndian.Uint32(cs)
	if fileChecksum != calculatedChecksum {
		return nil, fmt.Errorf("invalid metadata checksum")
	}

	off := 0
	i := 0
	for i < int(n) {
		bm := BlockMeta{}
		readed, err := bm.Decode(dataBuf)
		if err != nil {
			return nil, fmt.Errorf("failed to decode block metadata: %s", err)
		}
		off += readed

		metadata[i] = bm
		i += 1
	}

	return metadata, nil
}
