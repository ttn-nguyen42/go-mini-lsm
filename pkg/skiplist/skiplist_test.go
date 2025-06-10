package skiplist_test

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/ttn-nguyen42/go-mini-lsm/internal/types"
	"github.com/ttn-nguyen42/go-mini-lsm/pkg/skiplist"
)

func intCmp(a, b int) int {
	if a < b {
		return -1
	} else if a > b {
		return 1
	}
	return 0
}

func TestPut(t *testing.T) {
	sl, err := skiplist.New[int, int](intCmp)
	assert.NoError(t, err)

	sl.Put(1, 0)
	sl.Put(2, 3)

	v, ok := sl.Get(1)
	assert.True(t, ok)
	assert.Equal(t, 0, v)

	v, ok = sl.Get(2)
	assert.True(t, ok)
	assert.Equal(t, 3, v)
}

func TestDelete(t *testing.T) {
	sl, err := skiplist.New[int, int](intCmp)
	assert.NoError(t, err)

	sl.Put(1, 0)
	sl.Put(2, 3)

	ok := sl.Delete(1)
	assert.True(t, ok)

	sl.Put(5, 6)

	v, ok := sl.Get(2)
	assert.True(t, ok)
	assert.Equal(t, 3, v)

	v, ok = sl.Get(5)
	assert.True(t, ok)
	assert.Equal(t, 6, v)

	ok = sl.Delete(10)
	assert.False(t, ok)

	ok = sl.Delete(2)
	assert.True(t, ok)
	ok = sl.Delete(5)
	assert.True(t, ok)

	ok = sl.Delete(5)
	assert.False(t, ok)

	assert.True(t, sl.IsEmpty())
	assert.Equal(t, 0, sl.Size())
}

func TestIterator(t *testing.T) {
	sl, err := skiplist.New[int, int](intCmp)
	assert.NoError(t, err)

	sl.Put(1, 2)
	sl.Put(5, 6)
	sl.Put(3, 4)
	sl.Put(10, 15)
	sl.Put(2, 8)
	sl.Put(3, 10)

	expected := []int{1, 2, 3, 5, 10}
	var result []int

	iter := sl.Iter()
	defer iter.Close()

	for iter.HasNext() {
		result = append(result, iter.Key())
		iter.Next()
	}

	assert.Equal(t, expected, result)
}

func TestString(t *testing.T) {
	sl, err := skiplist.New[int, int](intCmp)
	assert.NoError(t, err)

	sl.Put(1, 2)
	sl.Put(5, 6)
	sl.Put(3, 4)
	sl.Put(10, 15)
	sl.Put(2, 8)
	sl.Put(3, 10)

	assert.NotPanics(t, func() {
		res := sl.String()
		assert.NotEmpty(t, res)
		fmt.Println(res)
	})
}

func TestScan(t *testing.T) {
	sl, err := skiplist.New[int, int](intCmp)
	assert.NoError(t, err)

	sl.Put(1, 2)
	sl.Put(5, 6)
	sl.Put(3, 4)
	sl.Put(10, 15)
	sl.Put(2, 8)
	sl.Put(3, 10)

	// Scan [2, 5]
	iter := sl.Scan(types.Include(2), types.Include(5))
	defer iter.Close()
	var keys []int
	for iter.HasNext() {
		keys = append(keys, iter.Key())
		iter.Next()
	}
	assert.Equal(t, []int{2, 3, 5}, keys)

	// Scan [3, 10]
	iter = sl.Scan(types.Include(3), types.Include(10))
	defer iter.Close()
	keys = nil
	for iter.HasNext() {
		keys = append(keys, iter.Key())
		iter.Next()
	}
	assert.Equal(t, []int{3, 5, 10}, keys)

	// Scan [0, 2]
	iter = sl.Scan(types.Include(0), types.Include(2))
	defer iter.Close()
	keys = nil
	for iter.HasNext() {
		keys = append(keys, iter.Key())
		iter.Next()
	}
	assert.Equal(t, []int{1, 2}, keys)

	// Scan (6, 9) (no results)
	iter = sl.Scan(types.Exclude(6), types.Exclude(9))
	defer iter.Close()
	keys = nil
	for iter.HasNext() {
		keys = append(keys, iter.Key())
		iter.Next()
	}
	assert.Empty(t, keys)
}

func TestScan_ExclusiveBounds(t *testing.T) {
	sl, err := skiplist.New[int, int](intCmp)
	assert.NoError(t, err)

	sl.Put(1, 2)
	sl.Put(5, 6)
	sl.Put(3, 4)
	sl.Put(10, 15)
	sl.Put(2, 8)
	sl.Put(3, 10)

	// Scan (2, 10) -- exclusive bounds
	iter := sl.Scan(types.Exclude(2), types.Exclude(10))
	defer iter.Close()
	var keys []int
	for iter.HasNext() {
		keys = append(keys, iter.Key())
		iter.Next()
	}
	assert.Equal(t, []int{3, 5}, keys)

	// Scan (1, 3) -- exclusive bounds
	iter = sl.Scan(types.Exclude(1), types.Exclude(3))
	defer iter.Close()
	keys = nil
	for iter.HasNext() {
		keys = append(keys, iter.Key())
		iter.Next()
	}
	assert.Equal(t, []int{2}, keys)

	// Scan (0, 1) -- exclusive bounds, should be empty
	iter = sl.Scan(types.Exclude(0), types.Exclude(1))
	defer iter.Close()
	keys = nil
	for iter.HasNext() {
		keys = append(keys, iter.Key())
		iter.Next()
	}
	assert.Empty(t, keys)
}
