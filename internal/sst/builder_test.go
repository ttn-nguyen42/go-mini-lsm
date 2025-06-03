package sst_test

import (
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
	defer table.File.Close()

	decoded, err := sst.Decode(42, table.File)
	assert.NoError(t, err)
	assert.Equal(t, table.Id, decoded.Id)
	assert.Equal(t, table.FirstKey, decoded.FirstKey)
	assert.Equal(t, table.LastKey, decoded.LastKey)
	assert.Equal(t, len(table.Blocks), len(decoded.Blocks))
	assert.NotNil(t, decoded.Filter)
}
