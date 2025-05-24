package lsm

import (
	"fmt"

	"github.com/ttn-nguyen42/go-mini-lsm/internal/types"
)

var ErrIterEnded error = fmt.Errorf("iterator has reached the end")

type Iterator interface {
	Key() types.Bytes
	Value() types.Bytes
	Next() error
	HasNext() bool
	Close()
}