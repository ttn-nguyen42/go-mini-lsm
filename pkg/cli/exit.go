package cli

import "io"

type exitCmd struct {
	buf io.Writer
}

func (c *exitCmd) ShouldRun(args []string) bool {
	return args[0] == "exit"
}

func (c *exitCmd) Execute(args []string) (bool, error) {
	if len(args) != 0 {
		return false, nil
	}

	_, err := c.buf.Write([]byte("Exiting REPL...\n"))
	if err != nil {
		return false, err
	}
	return false, nil
}
