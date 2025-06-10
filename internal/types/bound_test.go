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
	assert.True(t, IsWithinBoundary(l, r, 5, intCmp))   // inside
	assert.True(t, IsWithinBoundary(l, r, 1, intCmp))   // left included
	assert.True(t, IsWithinBoundary(l, r, 10, intCmp))  // right included
	assert.False(t, IsWithinBoundary(l, r, 0, intCmp))  // left out
	assert.False(t, IsWithinBoundary(l, r, 11, intCmp)) // right out

	// (1, 10)
	l = Exclude(1)
	r = Exclude(10)
	assert.True(t, IsWithinBoundary(l, r, 5, intCmp))
	assert.False(t, IsWithinBoundary(l, r, 1, intCmp))
	assert.False(t, IsWithinBoundary(l, r, 10, intCmp))

	// (1, 10]
	l = Exclude(1)
	r = Include(10)
	assert.True(t, IsWithinBoundary(l, r, 2, intCmp))
	assert.True(t, IsWithinBoundary(l, r, 10, intCmp))
	assert.False(t, IsWithinBoundary(l, r, 1, intCmp))

	// [1, 10)
	l = Include(1)
	r = Exclude(10)
	assert.True(t, IsWithinBoundary(l, r, 1, intCmp))
	assert.True(t, IsWithinBoundary(l, r, 9, intCmp))
	assert.False(t, IsWithinBoundary(l, r, 10, intCmp))
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

func TestAreBoundariesOverlap(t *testing.T) {
	cmp := func(a, b int) int { return a - b }

	// [1, 5] and [4, 10] => overlap
	l1, r1 := Include(1), Include(5)
	l2, r2 := Include(4), Include(10)
	assert.True(t, AreBoundariesOverlap(l1, r1, l2, r2, cmp))

	// [1, 5] and [6, 10] => no overlap
	l2, r2 = Include(6), Include(10)
	assert.False(t, AreBoundariesOverlap(l1, r1, l2, r2, cmp))

	// (1, 5) and (5, 10) => no overlap (exclusive at 5)
	l1, r1 = Exclude(1), Exclude(5)
	l2, r2 = Exclude(5), Exclude(10)
	assert.False(t, AreBoundariesOverlap(l1, r1, l2, r2, cmp))

	// [1, 5) and [5, 10] => no overlap (right exclusive, left inclusive)
	l1, r1 = Include(1), Exclude(5)
	l2, r2 = Include(5), Include(10)
	assert.False(t, AreBoundariesOverlap(l1, r1, l2, r2, cmp))

	// [1, 5] and [5, 10] => overlap at 5 (both inclusive)
	l1, r1 = Include(1), Include(5)
	l2, r2 = Include(5), Include(10)
	assert.True(t, AreBoundariesOverlap(l1, r1, l2, r2, cmp))

	// [1, 10] and [2, 3] => overlap (contained)
	l1, r1 = Include(1), Include(10)
	l2, r2 = Include(2), Include(3)
	assert.True(t, AreBoundariesOverlap(l1, r1, l2, r2, cmp))

	// [1, 2] and [3, 4] => no overlap
	l1, r1 = Include(1), Include(2)
	l2, r2 = Include(3), Include(4)
	assert.False(t, AreBoundariesOverlap(l1, r1, l2, r2, cmp))

	// l1 > r2: [6, 8] and [1, 5] => no overlap
	l1, r1 = Include(6), Include(8)
	l2, r2 = Include(1), Include(5)
	assert.False(t, AreBoundariesOverlap(l1, r1, l2, r2, cmp))

	// l1 > r2: (6, 8) and (1, 5) => no overlap (exclusive)
	l1, r1 = Exclude(6), Exclude(8)
	l2, r2 = Exclude(1), Exclude(5)
	assert.False(t, AreBoundariesOverlap(l1, r1, l2, r2, cmp))
}
