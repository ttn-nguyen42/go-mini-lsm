package types

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

type fakeIter struct {
	keys   []Bytes
	values []Bytes
	idx    int
}

func (f *fakeIter) HasNext() bool {
	return f.idx < len(f.keys)
}

func (f *fakeIter) Key() Bytes {
	return f.keys[f.idx]
}

func (f *fakeIter) Value() Bytes {
	return f.values[f.idx]
}

func (f *fakeIter) Next() error {
	f.idx++
	return nil
}

func TestTwoWayIter_MergeSorted(t *testing.T) {
	a := &fakeIter{
		keys:   []Bytes{[]byte("a"), []byte("c"), []byte("e")},
		values: []Bytes{[]byte("A"), []byte("C"), []byte("E")},
	}
	b := &fakeIter{
		keys:   []Bytes{[]byte("b"), []byte("c"), []byte("d")},
		values: []Bytes{[]byte("B"), []byte("C2"), []byte("D")},
	}
	iter := NewTwoWayIter(a, b, SkipOnDuplicate())

	var mergedKeys, mergedVals []string
	for iter.HasNext() {
		mergedKeys = append(mergedKeys, string(iter.Key()))
		mergedVals = append(mergedVals, string(iter.Value()))
		_ = iter.Next()
	}
	assert.Equal(t, []string{"a", "b", "c", "d", "e"}, mergedKeys)
	assert.Equal(t, []string{"A", "B", "C2", "D", "E"}, mergedVals)
}

func TestTwoWayIter_EmptyA(t *testing.T) {
	a := &fakeIter{keys: []Bytes{}, values: []Bytes{}}
	b := &fakeIter{keys: []Bytes{[]byte("x"), []byte("y")}, values: []Bytes{[]byte("X"), []byte("Y")}}
	iter := NewTwoWayIter(a, b)
	var keys []string
	for iter.HasNext() {
		keys = append(keys, string(iter.Key()))
		_ = iter.Next()
	}
	assert.Equal(t, []string{"x", "y"}, keys)
}

func TestTwoWayIter_EmptyB(t *testing.T) {
	a := &fakeIter{keys: []Bytes{[]byte("m"), []byte("n")}, values: []Bytes{[]byte("M"), []byte("N")}}
	b := &fakeIter{keys: []Bytes{}, values: []Bytes{}}
	iter := NewTwoWayIter(a, b)
	var keys []string
	for iter.HasNext() {
		keys = append(keys, string(iter.Key()))
		_ = iter.Next()
	}
	assert.Equal(t, []string{"m", "n"}, keys)
}
