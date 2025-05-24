package block

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/ttn-nguyen42/go-mini-lsm/internal/types"
)

func TestBlockEncodeDecodeRoundTrip(t *testing.T) {
	b := Builder(WithBlockSize(1024))
	b.Add(types.Bytes("foo"), types.Bytes("bar"))
	b.Add(types.Bytes("baz"), types.Bytes("qux"))
	blk := b.Build()

	encoded, err := Encode(&blk)
	assert.NoError(t, err)
	assert.NotEmpty(t, encoded)

	decoded, err := Decode(encoded)
	assert.NoError(t, err)
	assert.Equal(t, blk.offsets, decoded.offsets)
	assert.Equal(t, blk.data, decoded.data)
}

func TestBlockDecodeCorruptData(t *testing.T) {
	// Too short
	_, err := Decode(types.Bytes{})
	assert.Error(t, err)

	// Not enough for offsets
	_, err = Decode(types.Bytes{0, 1})
	assert.Error(t, err)

	// Odd offsets length
	b := Builder(WithBlockSize(1024))
	b.Add(types.Bytes("foo"), types.Bytes("bar"))
	blk := b.Build()
	encoded, _ := Encode(&blk)
	corrupt := append(encoded, 1) // add extra byte
	_, err = Decode(corrupt)
	assert.Error(t, err)
}
