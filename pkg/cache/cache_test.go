package cache

import (
	"testing"
	"github.com/stretchr/testify/assert"
)

func TestCacheSetGet(t *testing.T) {
	c := New[string, string](2)
	old, replaced := c.Set("a", "foo")
	assert.False(t, replaced)
	assert.Equal(t, "", old)
	val, ok := c.Get("a")
	assert.True(t, ok)
	assert.Equal(t, "foo", val)
	assert.Equal(t, len("a")+len("foo"), c.SizeBytes())
}

func TestCacheReplace(t *testing.T) {
	c := New[string, string](2)
	c.Set("a", "foo")
	old, replaced := c.Set("a", "bar")
	assert.True(t, replaced)
	assert.Equal(t, "foo", old)
	val, ok := c.Get("a")
	assert.True(t, ok)
	assert.Equal(t, "bar", val)
	assert.Equal(t, len("a")+len("bar"), c.SizeBytes())
}

func TestCacheEviction(t *testing.T) {
	c := New[string, string](2)
	c.Set("a", "foo")
	c.Set("b", "bar")
	c.Set("c", "baz") // should evict "a"
	_, ok := c.Get("a")
	assert.False(t, ok)
	val, ok := c.Get("b")
	assert.True(t, ok)
	assert.Equal(t, "bar", val)
	val, ok = c.Get("c")
	assert.True(t, ok)
	assert.Equal(t, "baz", val)
	expected := len("b")+len("bar") + len("c")+len("baz")
	assert.Equal(t, expected, c.SizeBytes())
}

func TestCacheDelete(t *testing.T) {
	c := New[string, string](2)
	c.Set("a", "foo")
	ok := c.Delete("a")
	assert.True(t, ok)
	_, ok = c.Get("a")
	assert.False(t, ok)
	ok = c.Delete("a")
	assert.False(t, ok)
	assert.Equal(t, 0, c.SizeBytes())
}

func TestCacheContains(t *testing.T) {
	c := New[string, string](2)
	c.Set("a", "foo")
	assert.True(t, c.Contains("a"))
	c.Delete("a")
	assert.False(t, c.Contains("a"))
}

func TestCacheClear(t *testing.T) {
	c := New[string, string](2)
	c.Set("a", "foo")
	c.Set("b", "bar")
	c.Clear()
	assert.Equal(t, 0, c.Len())
	assert.False(t, c.Contains("a"))
	assert.False(t, c.Contains("b"))
	assert.Equal(t, 0, c.SizeBytes())
}

func TestCacheGetOrSet(t *testing.T) {
	c := New[string, string](2)
	val, isSet := c.GetOrSet("a", "foo")
	assert.True(t, isSet)
	assert.Equal(t, "foo", val)
	val, isSet = c.GetOrSet("a", "bar")
	assert.False(t, isSet)
	assert.Equal(t, "foo", val)
}

func TestCacheLenCap(t *testing.T) {
	c := New[string, string](2)
	assert.Equal(t, 0, c.Len())
	assert.Equal(t, 2, c.Cap())
	c.Set("a", "foo")
	assert.Equal(t, 1, c.Len())
}

func TestCacheUpdateMostRecent(t *testing.T) {
	c := New[string, string](3)
	c.Set("a", "foo")
	c.Set("b", "bar")
	c.Set("c", "baz")
	// Access "b" to make it most recent
	c.Get("b")
	// Update the most recent key
	old, replaced := c.Set("b", "buzz")
	assert.True(t, replaced)
	assert.Equal(t, "bar", old)
	val, ok := c.Get("b")
	assert.True(t, ok)
	assert.Equal(t, "buzz", val)
	// SizeBytes should reflect updated value
	expected := len("a")+len("foo") + len("b")+len("buzz") + len("c")+len("baz")
	assert.Equal(t, expected, c.SizeBytes())
}

func TestCacheUpdateNotMostRecent(t *testing.T) {
	c := New[string, string](3)
	c.Set("a", "foo")
	c.Set("b", "bar")
	c.Set("c", "baz")
	// Access "c" to make it most recent, so "a" is not most recent
	c.Get("c")
	// Update a key that is not most recent ("a")
	old, replaced := c.Set("a", "fizz")
	assert.True(t, replaced)
	assert.Equal(t, "foo", old)
	val, ok := c.Get("a")
	assert.True(t, ok)
	assert.Equal(t, "fizz", val)
	// SizeBytes should reflect updated value
	expected := len("a")+len("fizz") + len("b")+len("bar") + len("c")+len("baz")
	assert.Equal(t, expected, c.SizeBytes())
}

func TestCacheEvictionHook(t *testing.T) {
	hookCalled := false
	var evictedKey, evictedVal string
	var evictedTS int64
	hook := func(key any, value any, ts int64) {
		hookCalled = true
		if k, ok := key.(string); ok {
			evictedKey = k
		}
		if v, ok := value.(string); ok {
			evictedVal = v
		}
		evictedTS = ts
	}

	c := New[string, string](2, WithEvictHook(hook))
	c.Set("a", "foo")
	c.Set("b", "bar")
	c.Set("c", "baz") // should evict "a"
	assert.True(t, hookCalled)
	assert.Equal(t, "a", evictedKey)
	assert.Equal(t, "foo", evictedVal)
	assert.NotZero(t, evictedTS)
}
