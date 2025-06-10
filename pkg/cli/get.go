package cli

import (
	"fmt"
	"io"

	"github.com/ttn-nguyen42/go-mini-lsm/internal/types"
	"github.com/ttn-nguyen42/go-mini-lsm/pkg/lsm"
)

type getCmd struct {
	lsm lsm.LSM
	buf io.Writer
}

func (c *getCmd) ShouldRun(args []string) bool {
	return args[0] == "get"
}

func (c *getCmd) Execute(args []string) (bool, error) {
	if len(args) != 1 {
		return true, fmt.Errorf("get command requires exactly 1 argument: <key>")
	}

	key := args[0]

	value, found, err := c.lsm.Get(types.Bytes(key))
	if err != nil {
		return false, fmt.Errorf("Failed to get key: %s", err)
	}
	if !found {
		fmt.Fprintf(c.buf, "Key %s not found\n", key)
		return true, nil
	}

	fmt.Fprintf(c.buf, "Key: %s, Value: %s\n", key, value)
	return true, nil
}
