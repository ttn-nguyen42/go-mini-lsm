package skiplist

type node[K, V any] struct {
	key   K
	value V
	cells []*cell[K, V]
}

func newNode[K, V any](level int) *node[K, V] {
	return &node[K, V]{
		cells: make([]*cell[K, V], 0, level),
	}
}

func (n *node[K, V]) getCell(level int) *cell[K, V] {
	if level < 0 || level >= len(n.cells) {
		return nil
	}

	return n.cells[level]
}
