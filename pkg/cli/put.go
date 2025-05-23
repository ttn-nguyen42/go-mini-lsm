package cli

import (
	"fmt"
	"io"

	"github.com/ttn-nguyen42/go-mini-lsm/internal/lsm"
	"github.com/ttn-nguyen42/go-mini-lsm/internal/types"
)

type putCmd struct {
	lsm lsm.LSM
	buf io.Writer
}

func (c *putCmd) ShouldRun(args []string) bool {
	return args[0] == "put"
}

func (c *putCmd) Execute(args []string) (bool, error) {
	if len(args) != 2 {
		return true, fmt.Errorf("put command requires exactly 2 arguments: <key> <value>")
	}

	key := args[0]
	value := args[1]

	c.lsm.Put(types.Bytes(key), types.Bytes(value))

	fmt.Fprintf(c.buf, "Put key: %s, value: %s\n", key, value)

	return true, nil
}
