package lsm_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/ttn-nguyen42/go-mini-lsm/internal/memtable"
	"github.com/ttn-nguyen42/go-mini-lsm/internal/types"
	"github.com/ttn-nguyen42/go-mini-lsm/pkg/lsm"
)

func newTestMemTable(id int, kvs ...[2]string) memtable.MemTable {
	t := memtable.New(id)
	for _, kv := range kvs {
		t.Put(types.Bytes(kv[0]), types.Bytes(kv[1]))
	}
	return t
}

func TestMergeIterSingleTable(t *testing.T) {
	tb := newTestMemTable(1, [2]string{"a", "1"}, [2]string{"b", "2"}, [2]string{"c", "3"})
	it := lsm.NewMergeIter([]memtable.MemTable{tb})
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

func TestMergeIterMultipleTables(t *testing.T) {
	tb1 := newTestMemTable(1, [2]string{"a", "1"}, [2]string{"c", "3"})
	tb2 := newTestMemTable(2, [2]string{"b", "2"}, [2]string{"d", "4"})
	it := lsm.NewMergeIter([]memtable.MemTable{tb1, tb2})
	defer it.Close()

	var keys, vals []string
	for it.HasNext() {
		keys = append(keys, string(it.Key()))
		vals = append(vals, string(it.Value()))
		_ = it.Next()
	}
	assert.Equal(t, []string{"a", "b", "c", "d"}, keys)
	assert.Equal(t, []string{"1", "2", "3", "4"}, vals)
}

func TestMergeIterDuplicateKeys(t *testing.T) {
	tb1 := newTestMemTable(1, [2]string{"a", "1"}, [2]string{"b", "2"})
	tb2 := newTestMemTable(2, [2]string{"b", "22"}, [2]string{"c", "3"})
	it := lsm.NewMergeIter([]memtable.MemTable{tb1, tb2})
	defer it.Close()

	var keys, vals []string
	for it.HasNext() {
		keys = append(keys, string(it.Key()))
		vals = append(vals, string(it.Value()))

		assert.Nil(t, it.Next())
	}
	// tb2 has higher id, so its value for 'b' should win
	assert.Equal(t, []string{"a", "b", "c"}, keys)
	assert.Equal(t, []string{"1", "22", "3"}, vals)
}

func TestMergeIterEmpty(t *testing.T) {
	it := lsm.NewMergeIter(nil)
	assert.False(t, it.HasNext())
}

func TestMergeIterEmptyCurrentMemTable(t *testing.T) {
	tb1 := newTestMemTable(1, [2]string{"a", "1"})
	tb2 := newTestMemTable(2, [2]string{"b", "2"})
	tb3 := newTestMemTable(3) // empty memtable
	it := lsm.NewMergeIter([]memtable.MemTable{tb1, tb2, tb3})
	defer it.Close()

	var keys, vals []string
	for it.HasNext() {
		keys = append(keys, string(it.Key()))
		vals = append(vals, string(it.Value()))
		_ = it.Next()
	}
	assert.Equal(t, []string{"a", "b"}, keys)
	assert.Equal(t, []string{"1", "2"}, vals)
}

func TestMergeIterCloseEarly(t *testing.T) {
	tb := newTestMemTable(1, [2]string{"a", "1"}, [2]string{"b", "2"}, [2]string{"c", "3"})
	it := lsm.NewMergeIter([]memtable.MemTable{tb})

	var keys, vals []string
	if it.HasNext() {
		keys = append(keys, string(it.Key()))
		vals = append(vals, string(it.Value()))
		it.Close() // Close before full iteration
	}
	// Only the first key/value should be collected
	assert.Equal(t, []string{"a"}, keys)
	assert.Equal(t, []string{"1"}, vals)
}

func TestMergeIterWithNoMemTablesKeyPanics(t *testing.T) {
	it := lsm.NewMergeIter([]memtable.MemTable{})
	defer func() {
		if r := recover(); r == nil {
			t.Errorf("Key() should panic when no current memtable")
		}
	}()
	_ = it.Key() // should panic
}

func TestMergeIterWithNoMemTablesValuePanics(t *testing.T) {
	it := lsm.NewMergeIter([]memtable.MemTable{})
	defer func() {
		if r := recover(); r == nil {
			t.Errorf("Value() should panic when no current memtable")
		}
	}()
	_ = it.Value() // should panic
}
