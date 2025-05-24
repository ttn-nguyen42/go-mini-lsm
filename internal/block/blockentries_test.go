package block

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/ttn-nguyen42/go-mini-lsm/internal/types"
)

func TestBlockEntries(t *testing.T) {
	b := Builder(WithBlockSize(1024))
	b.Add(types.Bytes("foo"), types.Bytes("bar"))
	b.Add(types.Bytes("baz"), types.Bytes("qux"))
	blk := b.Build()

	entries := blk.Entries()
	assert.Equal(t, 2, len(entries))
	assert.Equal(t, types.Bytes("foo"), entries[0].Key)
	assert.Equal(t, types.Bytes("bar"), entries[0].Value)
	assert.Equal(t, types.Bytes("baz"), entries[1].Key)
	assert.Equal(t, types.Bytes("qux"), entries[1].Value)
}
