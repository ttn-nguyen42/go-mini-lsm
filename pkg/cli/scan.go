package cli

import (
	"io"

	"github.com/ttn-nguyen42/go-mini-lsm/pkg/lsm"
)

type scanCmd struct {
	lsm lsm.LSM
	buf io.Writer
}

func (c *scanCmd) ShouldRun(args []string) bool {
	return args[0] == "scan"
}

func (c *scanCmd) Execute(args []string) (bool, error) {
	if len(args) != 0 {
		return true, nil
	}

	iter := c.lsm.Scan()
	defer iter.Close()

	for iter.HasNext() {
		key := iter.Key()
		value := iter.Value()
		c.buf.Write([]byte(key.String() + ": " + value.String() + "\n"))

		iter.Next()
	}

	return true, nil
}
