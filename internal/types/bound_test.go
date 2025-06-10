package types

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func intCmp(a, b int) int {
	return a - b
}

func TestBoundOverlap(t *testing.T) {
	// [1, 10]
	l := Include(1)
	r := Include(10)
	assert.True(t, BoundOverlap(l, r, 5, intCmp))   // inside
	assert.True(t, BoundOverlap(l, r, 1, intCmp))   // left included
	assert.True(t, BoundOverlap(l, r, 10, intCmp))  // right included
	assert.False(t, BoundOverlap(l, r, 0, intCmp))  // left out
	assert.False(t, BoundOverlap(l, r, 11, intCmp)) // right out

	// (1, 10)
	l = Exclude(1)
	r = Exclude(10)
	assert.True(t, BoundOverlap(l, r, 5, intCmp))
	assert.False(t, BoundOverlap(l, r, 1, intCmp))
	assert.False(t, BoundOverlap(l, r, 10, intCmp))

	// (1, 10]
	l = Exclude(1)
	r = Include(10)
	assert.True(t, BoundOverlap(l, r, 2, intCmp))
	assert.True(t, BoundOverlap(l, r, 10, intCmp))
	assert.False(t, BoundOverlap(l, r, 1, intCmp))

	// [1, 10)
	l = Include(1)
	r = Exclude(10)
	assert.True(t, BoundOverlap(l, r, 1, intCmp))
	assert.True(t, BoundOverlap(l, r, 9, intCmp))
	assert.False(t, BoundOverlap(l, r, 10, intCmp))
}

func TestIsBeforeAndIsAfter(t *testing.T) {
	intCmp := func(a, b int) int { return a - b }
	boundInc := Include(5)
	boundExc := Exclude(5)

	// IsBefore
	assert.True(t, boundInc.IsBefore(4, intCmp))  // 4 < 5 (inclusive)
	assert.False(t, boundInc.IsBefore(5, intCmp)) // 5 < 5 (inclusive) is false
	assert.False(t, boundInc.IsBefore(6, intCmp)) // 6 < 5 is false

	assert.True(t, boundExc.IsBefore(4, intCmp))  // 4 < 5 (exclusive)
	assert.True(t, boundExc.IsBefore(5, intCmp))  // 5 <= 5 (exclusive)
	assert.False(t, boundExc.IsBefore(6, intCmp)) // 6 <= 5 is false

	// IsAfter
	assert.False(t, boundInc.IsAfter(4, intCmp)) // 4 > 5 (inclusive) is false
	assert.False(t, boundInc.IsAfter(5, intCmp)) // 5 > 5 (inclusive) is false
	assert.True(t, boundInc.IsAfter(6, intCmp))  // 6 > 5 (inclusive)

	assert.False(t, boundExc.IsAfter(4, intCmp)) // 4 >= 5 (exclusive) is false
	assert.True(t, boundExc.IsAfter(5, intCmp))  // 5 >= 5 (exclusive)
	assert.True(t, boundExc.IsAfter(6, intCmp))  // 6 >= 5 (exclusive)
}
