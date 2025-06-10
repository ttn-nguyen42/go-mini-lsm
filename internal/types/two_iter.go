package types

import "bytes"

type Options struct {
	skipOnDuplicate bool
}

type Option func(*Options)

func SkipOnDuplicate() Option {
	return func(o *Options) {
		o.skipOnDuplicate = true
	}
}

type twoWayIter[A Iterator, B Iterator] struct {
	a       A
	b       B
	chooseA bool
	opts    *Options
}

func NewTwoWayIter[A Iterator, B Iterator](a A, b B, options ...Option) Iterator {
	opts := &Options{skipOnDuplicate: false}

	for _, opt := range options {
		opt(opts)
	}

	iter := &twoWayIter[A, B]{
		a:       a,
		b:       b,
		chooseA: false,
		opts:    opts,
	}
	if iter.opts.skipOnDuplicate {
		iter.skipAOnEqual()
	}
	iter.chooseA = iter.shouldChooseA()

	return iter
}

func (t *twoWayIter[A, B]) HasNext() bool {
	if t.chooseA {
		return t.a.HasNext()
	} else {
		return t.b.HasNext()
	}
}

func (t *twoWayIter[A, B]) Key() Bytes {
	if t.chooseA {
		return t.a.Key()
	} else {
		return t.b.Key()
	}
}

func (t *twoWayIter[A, B]) Next() error {
	var err error
	if t.chooseA {
		err = t.a.Next()
	} else {
		err = t.b.Next()
	}
	if err != nil {
		return err
	}

	if t.opts.skipOnDuplicate {
		t.skipAOnEqual()
	}
	t.chooseA = t.shouldChooseA()
	return nil
}

func (t *twoWayIter[A, B]) Value() Bytes {
	if t.chooseA {
		return t.a.Value()
	} else {
		return t.b.Value()
	}
}

func (t *twoWayIter[A, B]) shouldChooseA() bool {
	if !t.a.HasNext() {
		return false
	}
	if !t.b.HasNext() {
		return true
	}
	return bytes.Compare(t.a.Key(), t.b.Key()) <= 0
}

func (t *twoWayIter[A, B]) skipAOnEqual() {
	if t.a.HasNext() && t.b.HasNext() && bytes.Equal(t.a.Key(), t.b.Key()) {
		t.a.Next()
	}
}
