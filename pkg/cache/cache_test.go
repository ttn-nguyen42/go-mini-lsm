package cache

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/ttn-nguyen42/go-mini-lsm/internal/types"
)

func TestCacheSetGet(t *testing.T) {
	c := New[types.SizeableString, types.SizeableString](2)
	old, replaced := c.Set(types.SizeableString("a"), types.SizeableString("foo"))
	assert.False(t, replaced)
	assert.Equal(t, types.SizeableString(""), old)
	val, ok := c.Get(types.SizeableString("a"))
	assert.True(t, ok)
	assert.Equal(t, types.SizeableString("foo"), val)
	assert.Equal(t, len("a")+len("foo"), c.SizeBytes())
}

func TestCacheReplace(t *testing.T) {
	c := New[types.SizeableString, types.SizeableString](2)
	c.Set(types.SizeableString("a"), types.SizeableString("foo"))
	old, replaced := c.Set(types.SizeableString("a"), types.SizeableString("bar"))
	assert.True(t, replaced)
	assert.Equal(t, types.SizeableString("foo"), old)
	val, ok := c.Get(types.SizeableString("a"))
	assert.True(t, ok)
	assert.Equal(t, types.SizeableString("bar"), val)
	assert.Equal(t, len("a")+len("bar"), c.SizeBytes())
}

func TestCacheEviction(t *testing.T) {
	c := New[types.SizeableString, types.SizeableString](2)
	c.Set(types.SizeableString("a"), types.SizeableString("foo"))
	c.Set(types.SizeableString("b"), types.SizeableString("bar"))
	c.Set(types.SizeableString("c"), types.SizeableString("baz")) // should evict "a"
	_, ok := c.Get(types.SizeableString("a"))
	assert.False(t, ok)
	val, ok := c.Get(types.SizeableString("b"))
	assert.True(t, ok)
	assert.Equal(t, types.SizeableString("bar"), val)
	val, ok = c.Get(types.SizeableString("c"))
	assert.True(t, ok)
	assert.Equal(t, types.SizeableString("baz"), val)
	expected := len("b") + len("bar") + len("c") + len("baz")
	assert.Equal(t, expected, c.SizeBytes())
}

func TestCacheDelete(t *testing.T) {
	c := New[types.SizeableString, types.SizeableString](2)
	c.Set(types.SizeableString("a"), types.SizeableString("foo"))
	ok := c.Delete(types.SizeableString("a"))
	assert.True(t, ok)
	_, ok = c.Get(types.SizeableString("a"))
	assert.False(t, ok)
	ok = c.Delete(types.SizeableString("a"))
	assert.False(t, ok)
	assert.Equal(t, 0, c.SizeBytes())
}

func TestCacheContains(t *testing.T) {
	c := New[types.SizeableString, types.SizeableString](2)
	c.Set(types.SizeableString("a"), types.SizeableString("foo"))
	assert.True(t, c.Contains(types.SizeableString("a")))
	c.Delete(types.SizeableString("a"))
	assert.False(t, c.Contains(types.SizeableString("a")))
}

func TestCacheClear(t *testing.T) {
	c := New[types.SizeableString, types.SizeableString](2)
	c.Set(types.SizeableString("a"), types.SizeableString("foo"))
	c.Set(types.SizeableString("b"), types.SizeableString("bar"))
	c.Clear()
	assert.Equal(t, 0, c.Len())
	assert.False(t, c.Contains(types.SizeableString("a")))
	assert.False(t, c.Contains(types.SizeableString("b")))
	assert.Equal(t, 0, c.SizeBytes())
}

func TestCacheGetOrSet(t *testing.T) {
	c := New[types.SizeableString, types.SizeableString](2)
	val, isSet := c.GetOrSet(types.SizeableString("a"), func() (types.SizeableString, bool) { return "foo", true })
	assert.True(t, isSet)
	assert.Equal(t, types.SizeableString("foo"), val)
	val, isSet = c.GetOrSet(types.SizeableString("a"), func() (types.SizeableString, bool) { return "bar", true })
	assert.False(t, isSet)
	assert.Equal(t, types.SizeableString("foo"), val)
}

func TestCacheLenCap(t *testing.T) {
	c := New[types.SizeableString, types.SizeableString](2)
	assert.Equal(t, 0, c.Len())
	assert.Equal(t, 2, c.Cap())
	c.Set(types.SizeableString("a"), types.SizeableString("foo"))
	assert.Equal(t, 1, c.Len())
}

func TestCacheUpdateMostRecent(t *testing.T) {
	c := New[types.SizeableString, types.SizeableString](3)
	c.Set(types.SizeableString("a"), types.SizeableString("foo"))
	c.Set(types.SizeableString("b"), types.SizeableString("bar"))
	c.Set(types.SizeableString("c"), types.SizeableString("baz"))
	// Access "b" to make it most recent
	c.Get(types.SizeableString("b"))
	// Update the most recent key
	old, replaced := c.Set(types.SizeableString("b"), types.SizeableString("buzz"))
	assert.True(t, replaced)
	assert.Equal(t, types.SizeableString("bar"), old)
	val, ok := c.Get(types.SizeableString("b"))
	assert.True(t, ok)
	assert.Equal(t, types.SizeableString("buzz"), val)
	// SizeBytes should reflect updated value
	expected := len("a") + len("foo") + len("b") + len("buzz") + len("c") + len("baz")
	assert.Equal(t, expected, c.SizeBytes())
}

func TestCacheUpdateNotMostRecent(t *testing.T) {
	c := New[types.SizeableString, types.SizeableString](3)
	c.Set(types.SizeableString("a"), types.SizeableString("foo"))
	c.Set(types.SizeableString("b"), types.SizeableString("bar"))
	c.Set(types.SizeableString("c"), types.SizeableString("baz"))
	// Access "c" to make it most recent, so "a" is not most recent
	c.Get(types.SizeableString("c"))
	// Update a key that is not most recent ("a")
	old, replaced := c.Set(types.SizeableString("a"), types.SizeableString("fizz"))
	assert.True(t, replaced)
	assert.Equal(t, types.SizeableString("foo"), old)
	val, ok := c.Get(types.SizeableString("a"))
	assert.True(t, ok)
	assert.Equal(t, types.SizeableString("fizz"), val)
	// SizeBytes should reflect updated value
	expected := len("a") + len("fizz") + len("b") + len("bar") + len("c") + len("baz")
	assert.Equal(t, expected, c.SizeBytes())
}

func TestCacheEvictionHook(t *testing.T) {
	hookCalled := false
	var evictedKey, evictedVal types.SizeableString
	var evictedTS int64
	hook := func(key any, value any, ts int64) {
		hookCalled = true
		if k, ok := key.(types.SizeableString); ok {
			evictedKey = k
		}
		if v, ok := value.(types.SizeableString); ok {
			evictedVal = v
		}
		evictedTS = ts
	}

	c := New[types.SizeableString, types.SizeableString](2, WithEvictHook(hook))
	c.Set(types.SizeableString("a"), types.SizeableString("foo"))
	c.Set(types.SizeableString("b"), types.SizeableString("bar"))
	c.Set(types.SizeableString("c"), types.SizeableString("baz")) // should evict "a"
	assert.True(t, hookCalled)
	assert.Equal(t, types.SizeableString("a"), evictedKey)
	assert.Equal(t, types.SizeableString("foo"), evictedVal)
	assert.NotZero(t, evictedTS)
}
