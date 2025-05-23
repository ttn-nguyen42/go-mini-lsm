package cli

import (
	"fmt"
	"io"
)

type helpCmd struct {
	buf io.Writer
}

func (c *helpCmd) ShouldRun(args []string) bool {
	return args[0] == "help"
}

func (c *helpCmd) Execute(args []string) (bool, error) {
	if len(args) != 0 {
		return true, fmt.Errorf("help command does not take any arguments")
	}

	helpText := `
Available commands:
1. help: Show this help message
2. exit: Exit the REPL
3. put <key> <value>: Store a key-value pair
4. get <key>: Retrieve the value for a given key
5. del <key>: Delete a key-value pair
`

	_, err := c.buf.Write([]byte(helpText))
	if err != nil {
		return false, err
	}

	return true, nil
}
