package heap

import "sync"

type Comparator[T any] func(a, b T) int

type Heap[T any] interface {
	Push(item T)
	Pop() T
	Peek() T
	Len() int
	Elems() []T
}

type minHeap[T any] struct {
	lock sync.RWMutex

	heap []T
	cmp  Comparator[T]
	size int
}

func MinHeap[T any](cmp Comparator[T]) Heap[T] {
	return &minHeap[T]{
		heap: make([]T, 0),
		cmp:  cmp,
	}
}

func MinHeapSeeded[T any](cmp Comparator[T], items []T) Heap[T] {
	mh := &minHeap[T]{
		heap: make([]T, 0),
		cmp:  cmp,
		size: len(items),
	}

	mh.heap = append(mh.heap, items...)

	for i := (mh.size/2 - 1); i >= 0; i-- {
		mh.siftDown(i)
	}

	return mh
}

func (h *minHeap[T]) Len() int {
	h.lock.RLock()
	defer h.lock.RUnlock()

	return h.size
}

func (h *minHeap[T]) Peek() T {
	h.lock.RLock()
	defer h.lock.RUnlock()

	return h.peek()
}

func (h *minHeap[T]) peek() T {
	if len(h.heap) == 0 {
		var empty T
		return empty
	}

	return h.heap[0]
}

func (h *minHeap[T]) Pop() T {
	h.lock.Lock()
	defer h.lock.Unlock()

	return h.pop()
}

func (h *minHeap[T]) pop() T {
	if len(h.heap) == 0 {
		var empty T
		return empty
	}

	if len(h.heap) == 1 {
		item := h.heap[0]
		h.heap = h.heap[:0]
		h.size = 0
		return item
	}

	root := h.heap[0]
	last := h.heap[len(h.heap)-1]

	h.heap[0] = last
	h.size -= 1
	h.heap = h.heap[:len(h.heap)-1]

	h.siftDown(0)
	return root
}

func (h *minHeap[T]) siftDown(i int) {
	left := h.left(i)
	right := h.right(i)
	smallest := i

	if left < h.size && h.cmp(h.heap[left], h.heap[i]) < 0 {
		smallest = left
	}

	if right < h.size && h.cmp(h.heap[right], h.heap[smallest]) < 0 {
		smallest = right
	}

	if smallest != i {
		h.swap(i, smallest)

		h.siftDown(smallest)
	}
}

func (h *minHeap[T]) Push(item T) {
	h.lock.Lock()
	defer h.lock.Unlock()

	h.push(item)
}

func (h *minHeap[T]) push(item T) {
	h.heap = append(h.heap, item)
	h.size += 1

	h.siftUp()
}

func (h *minHeap[T]) siftUp() {
	for i := h.size - 1; i > 0 && h.cmp(h.heap[h.parent(i)], h.heap[i]) > 0; i = h.parent(i) {
		h.swap(i, h.parent(i))
	}
}

func (h *minHeap[T]) swap(i, j int) {
	h.heap[i], h.heap[j] = h.heap[j], h.heap[i]
}

func (h *minHeap[T]) parent(i int) int {
	return (i - 1) / 2
}

func (h *minHeap[T]) left(i int) int {
	return 2*i + 1
}

func (h *minHeap[T]) right(i int) int {
	return 2*i + 2
}

func (h *minHeap[T]) Elems() []T {
	h.lock.RLock()
	defer h.lock.RUnlock()

	return h.elems()
}

func (h *minHeap[T]) elems() []T {
	return h.heap
}
