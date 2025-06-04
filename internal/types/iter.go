package types

type Iterator interface {
	Key() Bytes
	Value() Bytes
	HasNext() bool
	Next() error
}

type ClosableIterator interface {
	Iterator
	Close()
}

type SeekableIterator interface {
	Iterator
	Seek(idx int) error
	SeekToKey(key Bytes) error
}
