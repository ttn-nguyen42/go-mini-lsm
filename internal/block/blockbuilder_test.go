package block

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/ttn-nguyen42/go-mini-lsm/internal/types"
)

func TestBlockBuilderAddAndBuild(t *testing.T) {
	b := NewBuilder(WithBlockSize(1024))
	assert.True(t, b.IsEmpty())

	ok := b.Add(types.Bytes("foo"), types.Bytes("bar"))
	assert.True(t, ok)
	assert.False(t, b.IsEmpty())

	ok = b.Add(types.Bytes("baz"), types.Bytes("qux"))
	assert.True(t, ok)

	blk := b.Build()
	assert.Equal(t, 2, len(blk.offsets))
	assert.NotEmpty(t, blk.data)
}

func TestBlockBuilderAddFull(t *testing.T) {
	b := NewBuilder(WithBlockSize(32))
	ok := b.Add(types.Bytes("a"), types.Bytes("b"))
	assert.True(t, ok)
	// This should fill the block
	ok = b.Add(types.Bytes("01234567890123456789"), types.Bytes("01234567890123456789"))
	assert.False(t, ok)
}
