package types

type Bytes []byte

func (b Bytes) Size() int {
	return len(b)
}
