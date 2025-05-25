package block

import (
	"fmt"
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
	assert.NoError(t, err)
	assert.False(t, it.HasNext())
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

func TestBlockIteratorSearch(t *testing.T) {
	b := NewBuilder(WithBlockSize(4096))
	// Add 100 keys: k000, k001, ..., k099
	for i := 0; i < 100; i += 1 {
		key := types.Bytes(fmt.Sprintf("k%03d", i))
		val := types.Bytes(fmt.Sprintf("v%03d", i))
		b.Add(key, val)
	}
	blk := b.Build()
	it := blk.Scan()
	defer func() {
		if c, ok := it.(interface{ Close() }); ok {
			c.Close()
		}
	}()

	// Seek to a key in the middle
	midKey := types.Bytes("k050")
	err := it.SeekToKey(midKey)
	assert.NoError(t, err)
	assert.Equal(t, midKey, it.Key())
	assert.Equal(t, types.Bytes("v050"), it.Value())

	// Seek to a key that does not exist but is between two keys
	nonExistKey := types.Bytes("k055x")
	err = it.SeekToKey(nonExistKey)
	assert.NoError(t, err)
	// Should land on the next greater key, which is k056
	assert.Equal(t, types.Bytes("k056"), it.Key())

	// Seek to a key before all keys
	err = it.SeekToKey(types.Bytes("a000"))
	assert.NoError(t, err)
	assert.Equal(t, types.Bytes("k000"), it.Key())

	// Seek to a key after all keys
	err = it.SeekToKey(types.Bytes("z999"))
	assert.True(t, it.HasNext())
	assert.NoError(t, err)
	assert.Equal(t, types.Bytes("k099"), it.Key())
}
