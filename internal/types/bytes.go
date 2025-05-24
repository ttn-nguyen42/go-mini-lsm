package types

type Bytes []byte

func (b Bytes) Size() int {
	return len(b)
}

func (b Bytes) String() string {
	return string(b)
}