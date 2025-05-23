package skiplist

type cell[K, V any] struct {
	prev *node[K, V]
	next *node[K, V]
}

func newCell[K, V any](prev, next *node[K, V]) *cell[K, V] {
	return &cell[K, V]{
		prev: prev,
		next: next,
	}
}