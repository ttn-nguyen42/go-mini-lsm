package memtable

import (
	"github.com/stretchr/testify/assert"
	"github.com/ttn-nguyen42/go-mini-lsm/internal/types"
	"testing"
)

func TestMemTableIter(t *testing.T) {
	m := New(1)
	m.Put([]byte("a"), []byte("A"))
	m.Put([]byte("b"), []byte("B"))
	m.Put([]byte("c"), []byte("C"))
	m.Put([]byte("d"), []byte("D"))
	m.Put([]byte("e"), []byte("E"))

	// Full scan
	it := m.Iter()
	var keys, vals []string
	for it.HasNext() {
		keys = append(keys, string(it.Key()))
		vals = append(vals, string(it.Value()))
		it.Next()
	}
	it.Close()
	assert.Equal(t, []string{"a", "b", "c", "d", "e"}, keys)
	assert.Equal(t, []string{"A", "B", "C", "D", "E"}, vals)

	// Range scan (b, d)
	it = m.Scan(types.Exclude(types.Bytes("b")), types.Exclude(types.Bytes("d")))
	keys = nil
	for it.HasNext() {
		keys = append(keys, string(it.Key()))
		it.Next()
	}
	it.Close()
	assert.Equal(t, []string{"c"}, keys)

	// Empty scan
	it = m.Scan(types.Exclude(types.Bytes("e")), types.Exclude(types.Bytes("z")))
	keys = nil
	for it.HasNext() {
		keys = append(keys, string(it.Key()))
		it.Next()
	}
	it.Close()
	assert.Empty(t, keys)
}
