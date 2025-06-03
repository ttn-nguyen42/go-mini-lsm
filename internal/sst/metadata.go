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

func (b *BlockMeta) Encode(data []byte) []byte {
	data = binary.BigEndian.AppendUint32(data, b.Offset)
	data = binary.BigEndian.AppendUint16(data, uint16(len(b.FirstKey)))
	data = append(data, b.FirstKey...)
	data = binary.BigEndian.AppendUint16(data, uint16(len(b.LastKey)))
	data = append(data, b.LastKey...)

	return data
}

// +-------------------+-------------------+----------------+
// | # of blocks (4b)  |  metadata blocks  |  CRC32 cs (4b) |
// +-------------------+-------------------+----------------+
func encodeBlockMetadatas(data []byte, metadata []BlockMeta) []byte {
	curOffset := len(data)

	data = binary.BigEndian.AppendUint32(data, uint32(len(metadata))) // number of metadata blocks

	for _, m := range metadata {
		data = m.Encode(data)
	} // metadata

	h := crc32.ChecksumIEEE(data[curOffset+4:])
	data = binary.BigEndian.AppendUint32(data, h) // checksum
	return data
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

func (m *BlockMeta) Size() int {
	offsetSize := 4

	keyLengthSize := 2
	keySize := len(m.FirstKey)
	valLengthSize := 2
	valSize := len(m.LastKey)

	return offsetSize + keyLengthSize + keySize + valLengthSize + valSize
}
