package types

type Bound[T any] struct {
	data     T
	included bool
}

func (b Bound[T]) Data() T {
	return b.data
}

// IsBefore returns true if data is in the left side of the bound
func (b Bound[T]) IsBefore(data T, cmp Comparator[T]) bool {
	c := cmp(data, b.data)
	if b.included {
		return c < 0
	} else {
		return c <= 0
	}
}

// IsAfter returns true if data is in the right side of the bound
func (b Bound[T]) IsAfter(data T, cmp Comparator[T]) bool {
	c := cmp(data, b.data)
	if b.included {
		return c > 0
	} else {
		return c >= 0
	}
}

func Include[T any](data T) Bound[T] {
	return Bound[T]{
		data:     data,
		included: true,
	}
}

func Exclude[T any](data T) Bound[T] {
	return Bound[T]{
		data:     data,
		included: false,
	}
}

type Comparator[T any] func(a, b T) int

func IsWithinBoundary[T any](l Bound[T], r Bound[T], data T, compare Comparator[T]) bool {
	compareLower := compare(data, l.data)
	if l.included {
		if compareLower < 0 {
			return false
		}
	} else {
		if compareLower <= 0 {
			return false
		}
	}
	compareUpper := compare(data, r.data)
	if r.included {
		if compareUpper > 0 {
			return false
		}
	} else {
		if compareUpper >= 0 {
			return false
		}
	}

	return true
}

func IsWithinRange[T any](l T, r T, data T, compare Comparator[T]) bool {
	lb := Include(l)
	rb := Include(r)

	return IsWithinBoundary(lb, rb, data, compare)
}

func AreBoundariesOverlap[T any](l1 Bound[T], r1 Bound[T], l2 Bound[T], r2 Bound[T], cmp Comparator[T]) bool {
	// If l1 > r2, intervals do not overlap
	cmpl1r2 := cmp(l1.data, r2.data)
	if cmpl1r2 > 0 || (cmpl1r2 == 0 && (!l1.included || !r2.included)) {
		return false
	}
	// If l2 > r1, intervals do not overlap
	cmpl2r1 := cmp(l2.data, r1.data)
	if cmpl2r1 > 0 || (cmpl2r1 == 0 && (!l2.included || !r1.included)) {
		return false
	}
	return true
}
