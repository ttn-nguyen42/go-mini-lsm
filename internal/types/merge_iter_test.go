package types_test

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/ttn-nguyen42/go-mini-lsm/internal/types"
)

type mockIter struct {
	keys []types.Bytes
	vals []types.Bytes
	idx  int
}

func (m *mockIter) HasNext() bool {
	return m.idx < len(m.keys)
}

func (m *mockIter) Key() types.Bytes {
	return m.keys[m.idx]
}

func (m *mockIter) Value() types.Bytes {
	return m.vals[m.idx]
}

func (m *mockIter) Next() error {
	m.idx += 1
	return nil
}

func TestMergeIter_Simple(t *testing.T) {
	it1 := &mockIter{
		keys: []types.Bytes{types.Bytes("a"), types.Bytes("c")},
		vals: []types.Bytes{types.Bytes("A"), types.Bytes("C")},
	}
	it2 := &mockIter{
		keys: []types.Bytes{types.Bytes("b"), types.Bytes("d")},
		vals: []types.Bytes{types.Bytes("B"), types.Bytes("D")},
	}
	merge := types.NewMergeIter(it1, it2)
	var keys, vals []string
	for merge.HasNext() {
		keys = append(keys, string(merge.Key()))
		vals = append(vals, string(merge.Value()))
		fmt.Println(merge.Key())
		merge.Next()
	}
	assert.Equal(t, []string{"a", "b", "c", "d"}, keys)
	assert.Equal(t, []string{"A", "B", "C", "D"}, vals)
}

func TestMergeIter_Duplicates(t *testing.T) {
	it1 := &mockIter{
		keys: []types.Bytes{types.Bytes("a"), types.Bytes("b"), types.Bytes("c")},
		vals: []types.Bytes{types.Bytes("A1"), types.Bytes("B1"), types.Bytes("C1")},
	}
	it2 := &mockIter{
		keys: []types.Bytes{types.Bytes("b"), types.Bytes("c"), types.Bytes("d")},
		vals: []types.Bytes{types.Bytes("B2"), types.Bytes("C2"), types.Bytes("D2")},
	}
	merge := types.NewMergeIter(it1, it2)
	var keys, vals []string
	for merge.HasNext() {
		keys = append(keys, string(merge.Key()))
		vals = append(vals, string(merge.Value()))
		merge.Next()
	}
	// Should deduplicate b and c, prefer it1 (lower id)
	assert.Equal(t, []string{"a", "b", "c", "d"}, keys)
	assert.Equal(t, []string{"A1", "B1", "C1", "D2"}, vals)
}

func TestMergeIter_Empty(t *testing.T) {
	merge := types.NewMergeIter()
	assert.False(t, merge.HasNext())
}
