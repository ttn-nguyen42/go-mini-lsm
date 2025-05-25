package block

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/ttn-nguyen42/go-mini-lsm/internal/types"
)

func TestBlockIteratorBasic(t *testing.T) {
	b := NewBuilder(WithBlockSize(1024))
	b.Add(types.Bytes("foo"), types.Bytes("bar"))
	b.Add(types.Bytes("baz"), types.Bytes("qux"))
	blk := b.Build()
	it := blk.Scan()
	defer func() {
		if c, ok := it.(interface{ Close() }); ok {
			c.Close()
		}
	}()

	var keys, values []string
	for it.HasNext() {
		keys = append(keys, string(it.Key()))
		values = append(values, string(it.Value()))
		assert.NoError(t, it.Next())
	}
	assert.Equal(t, []string{"foo", "baz"}, keys)
	assert.Equal(t, []string{"bar", "qux"}, values)
}

func TestBlockIteratorEmpty(t *testing.T) {
	b := NewBuilder(WithBlockSize(1024))
	blk := b.Build()
	it := blk.Scan()
	defer func() {
		if c, ok := it.(interface{ Close() }); ok {
			c.Close()
		}
	}()
	assert.False(t, it.HasNext())
}

func TestBlockIteratorNextAfterEnd(t *testing.T) {
	b := NewBuilder(WithBlockSize(1024))
	b.Add(types.Bytes("foo"), types.Bytes("bar"))
	blk := b.Build()
	it := blk.Scan()
	defer func() {
		if c, ok := it.(interface{ Close() }); ok {
			c.Close()
		}
	}()
	assert.True(t, it.HasNext())
	assert.NoError(t, it.Next())
	assert.False(t, it.HasNext())
	err := it.Next()
	assert.Error(t, err)
}

func TestBlockIteratorKeyValuePanics(t *testing.T) {
	b := NewBuilder(WithBlockSize(1024))
	b.Add(types.Bytes("foo"), types.Bytes("bar"))
	blk := b.Build()
	it := blk.Scan()
	defer func() {
		if c, ok := it.(interface{ Close() }); ok {
			c.Close()
		}
	}()
	it.Next() // move to end
	assert.False(t, it.HasNext())
	assert.Panics(t, func() { _ = it.Key() })
	assert.Panics(t, func() { _ = it.Value() })
}
