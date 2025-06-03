package sst_test

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/ttn-nguyen42/go-mini-lsm/internal/sst"
	"github.com/ttn-nguyen42/go-mini-lsm/internal/types"
)

func TestBuilderEncodeDecodeSimple(t *testing.T) {
	b := sst.NewBuilder(128)
	for i := range 3 {
		key := types.Bytes([]byte{byte('k' + i)})
		val := types.Bytes([]byte{byte('v' + i)})
		assert.NoError(t, b.Add(key, val))
	}

	table, err := b.Build(42, "sstable-simple-test.sst")
	assert.NoError(t, err)
	assert.NotNil(t, table)
	defer table.Close()
	defer os.Remove("sstable-simple-test.sst")

	decoded, err := sst.Decode(42, table.File())
	assert.NoError(t, err)
	assert.Equal(t, table.Id(), decoded.Id())
	assert.Equal(t, table.FirstKey(), decoded.FirstKey())
	assert.Equal(t, table.LastKey(), decoded.LastKey())
}

func TestSSTIteratorBasic(t *testing.T) {
	b := sst.NewBuilder(128)
	for i := range 5 {
		key := types.Bytes([]byte{byte('a' + i)})
		val := types.Bytes([]byte{byte('A' + i)})
		assert.NoError(t, b.Add(key, val))
	}
	tmpfile, err := os.CreateTemp("", "sstable-iter-*.sst")
	assert.NoError(t, err)
	defer os.Remove(tmpfile.Name())
	table, err := b.Build(1, tmpfile.Name())
	assert.NoError(t, err)
	assert.NotNil(t, table)

	it, err := table.Scan()
	assert.NoError(t, err)
	var keys, vals []string
	for it.HasNext() {
		keys = append(keys, string(it.Key()))
		vals = append(vals, string(it.Value()))
		it.Next()
	}
	assert.Equal(t, []string{"a", "b", "c", "d", "e"}, keys)
	assert.Equal(t, []string{"A", "B", "C", "D", "E"}, vals)
}

func TestSSTIteratorEmpty(t *testing.T) {
	b := sst.NewBuilder(128)
	tmpfile, err := os.CreateTemp("", "sstable-iter-empty-*.sst")
	assert.NoError(t, err)
	defer os.Remove(tmpfile.Name())
	table, err := b.Build(1, tmpfile.Name())
	assert.NoError(t, err)
	it, err := table.Scan()
	assert.NoError(t, err)
	assert.Error(t, sst.ErrIterEnd, it.HasNext())
}

func TestSSTIteratorMultiBlock(t *testing.T) {
	b := sst.NewBuilder(32)
	for i := range 20 {
		key := types.Bytes([]byte{byte('a' + i)})
		val := types.Bytes([]byte{byte('A' + i)})
		_ = b.Add(key, val)
	}
	tmpfile, err := os.CreateTemp("", "sstable-iter-multiblock-*.sst")
	assert.NoError(t, err)
	defer os.Remove(tmpfile.Name())
	table, err := b.Build(1, tmpfile.Name())
	assert.NoError(t, err)
	it, err := table.Scan()
	assert.NoError(t, err)
	var count int
	for it.HasNext() {
		_ = it.Key()
		_ = it.Value()
		it.Next()
		count += 1
	}
	assert.Equal(t, 20, count)
}
