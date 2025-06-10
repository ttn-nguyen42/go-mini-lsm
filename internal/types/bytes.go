package types

import "bytes"

type Bytes []byte

func (b Bytes) Size() int {
	return len(b)
}

func (b Bytes) String() string {
	return string(b)
}

type SizeableString string

func (b SizeableString) Size() int {
	return len(b)
}

func BytesComparator(a, b Bytes) int {
	return bytes.Compare(a, b)
}