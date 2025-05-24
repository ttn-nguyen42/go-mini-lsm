package heap_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/ttn-nguyen42/go-mini-lsm/pkg/heap"
)

func intCmp(a, b int) int {
	if a < b {
		return -1
	} else if a > b {
		return 1
	}
	return 0
}

func TestMinHeapPushPeekPop(t *testing.T) {
	h := heap.MinHeap(intCmp)

	assert.Equal(t, 0, h.Len())

	h.Push(3)
	h.Push(1)
	h.Push(4)
	h.Push(2)

	assert.Equal(t, 4, h.Len())
	assert.Equal(t, 1, h.Peek())

	assert.Equal(t, 1, h.Pop())
	assert.Equal(t, 2, h.Pop())
	assert.Equal(t, 3, h.Pop())
	assert.Equal(t, 4, h.Pop())
	assert.Equal(t, 0, h.Len())
}

func TestMinHeapEmptyPeekPop(t *testing.T) {
	h := heap.MinHeap(intCmp)

	assert.Equal(t, 0, h.Len())
	assert.Equal(t, 0, h.Peek())
	assert.Equal(t, 0, h.Pop())
}

func TestMinHeapMixedOps(t *testing.T) {
	h := heap.MinHeap(intCmp)

	h.Push(10)
	h.Push(5)
	h.Push(7)
	assert.Equal(t, 3, h.Len())
	assert.Equal(t, 5, h.Peek())

	h.Push(2)
	assert.Equal(t, 2, h.Peek())
	assert.Equal(t, 2, h.Pop())
	assert.Equal(t, 5, h.Pop())
	assert.Equal(t, 7, h.Pop())
	assert.Equal(t, 10, h.Pop())
	assert.Equal(t, 0, h.Len())
}

func TestMinHeapSiftDownRightChild(t *testing.T) {
	h := heap.MinHeap(intCmp)
	// Create a heap where the right child is the smallest
	// Heap array: [10, 20, 5] (10 is root, 20 is left, 5 is right)
	h.Push(10)
	h.Push(20)
	h.Push(5)
	// After push, heap should reorder to [5, 20, 10] if heap property is maintained
	// But we want to test the siftDown logic directly, so pop the root and check
	min := h.Pop()
	assert.Equal(t, 5, min)
	// The next pop should be 10, then 20
	assert.Equal(t, 10, h.Pop())
	assert.Equal(t, 20, h.Pop())
}

func TestMinHeapSeeded(t *testing.T) {
	items := []int{5, 1, 4, 2, 3}
	h := heap.MinHeapSeeded(intCmp, items)
	assert.Equal(t, 5, h.Len())
	// Should pop in sorted order
	for i := 1; i <= 5; i++ {
		assert.Equal(t, i, h.Pop())
	}
	assert.Equal(t, 0, h.Len())
}
