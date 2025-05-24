package lsm_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/ttn-nguyen42/go-mini-lsm/internal/memtable"
	"github.com/ttn-nguyen42/go-mini-lsm/pkg/lsm"
)

func TestLsmIterDeletedKeyNotVisible(t *testing.T) {
	tb1 := newTestMemTable(1, [2]string{"a", "1"}, [2]string{"b", "2"})
	tb2 := newTestMemTable(2, [2]string{"b", ""}, [2]string{"c", "3"}) // 'b' is deleted in tb2
	it := lsm.NewIter([]memtable.MemTable{tb1, tb2})
	defer it.Close()

	var keys, vals []string
	for it.HasNext() {
		k := string(it.Key())
		v := string(it.Value())
		keys = append(keys, k)
		vals = append(vals, v)
		_ = it.Next()
	}
	// 'b' should not appear, only 'a' and 'c'
	assert.Equal(t, []string{"a", "c"}, keys)
	assert.Equal(t, []string{"1", "3"}, vals)
}

func TestLsmIterAllDeleted(t *testing.T) {
	tb1 := newTestMemTable(1, [2]string{"a", "1"})
	tb2 := newTestMemTable(2, [2]string{"a", ""}) // 'a' is deleted
	it := lsm.NewIter([]memtable.MemTable{tb1, tb2})
	defer it.Close()

	var keys []string
	for it.HasNext() {
		keys = append(keys, string(it.Key()))
		_ = it.Next()
	}

	assert.Empty(t, keys)
}

func TestLsmIterBasic(t *testing.T) {
	tb1 := newTestMemTable(1, [2]string{"a", "1"}, [2]string{"b", "2"})
	tb2 := newTestMemTable(2, [2]string{"c", "3"})
	it := lsm.NewIter([]memtable.MemTable{tb1, tb2})
	defer it.Close()

	var keys, vals []string
	for it.HasNext() {
		keys = append(keys, string(it.Key()))
		vals = append(vals, string(it.Value()))
		_ = it.Next()
	}
	assert.Equal(t, []string{"a", "b", "c"}, keys)
	assert.Equal(t, []string{"1", "2", "3"}, vals)
}
