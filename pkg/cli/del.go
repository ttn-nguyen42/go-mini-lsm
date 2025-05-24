package cli

import (
	"fmt"
	"io"

	"github.com/ttn-nguyen42/go-mini-lsm/pkg/lsm"
	"github.com/ttn-nguyen42/go-mini-lsm/internal/types"
)

type delCmd struct {
	lsm lsm.LSM
	buf io.Writer
}

func (c *delCmd) ShouldRun(args []string) bool {
	return args[0] == "del"
}

func (c *delCmd) Execute(args []string) (bool, error) {
	if len(args) != 1 {
		return true, fmt.Errorf("del command requires exactly 1 argument: <key>")
	}

	key := args[0]

	c.lsm.Delete(types.Bytes(key))

	fmt.Fprintf(c.buf, "Deleted key: %s\n", key)
	return true, nil
}
